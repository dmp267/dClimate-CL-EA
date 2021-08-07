# Gets all gridded dataset classes from the datasets module
import ipfshttpclient
import os, pickle, math, requests, datetime, io, gzip, json, logging, csv, tarfile
import datetime, pytz, csv, inspect
from astropy import units as u
import numpy as np
import pandas as pd
from timezonefinder import TimezoneFinder
import simple_gridded_datasets
from extra_tools import convert_nans_to_none, cpc_lat_lon_to_conventional, conventional_lat_lon_to_cpc, tupleify, METRIC_TO_IMPERIAL as M2I, IMPERIAL_TO_METRIC as I2M, UNIT_ALIASES, get_heads, IPFSError, DatasetError, CoordinateNotFoundError, GATEWAY_URL
# from bridge import Bridge

GRIDDED_DATASETS = {
    obj.dataset: obj for obj in vars(simple_gridded_datasets).values()
    if inspect.isclass(obj) and type(obj.dataset) == str
}

def get_metadata_http(hash_str, url=GATEWAY_URL):
    """
    Get the metadata file for a given hash.
    Args:
        url (str): the url of the IPFS server
        hash_str (str): the hash of the ipfs dataset
    Returns (example metadata.json):

        {
            'date range': [
                '1981/01/01',
                '2019/07/31'
            ],
            'entry delimiter': ',',
            'latitude range': [
                -49.975, 49.975
            ],
            'longitude range': [
                -179.975, 179.975]
            ,
            'name': 'CHIRPS .05 Daily Full Set Uncompressed',
            'period': 'daily',
            'precision': 0.01,
            'resolution': 0.05,
            'unit of measurement': 'mm',
            'year delimiter': '\n'
        }
    """
    # http_bridge = Bridge()
    metadata_url = "%s/ipfs/%s/metadata.json" % (url, hash_str)
    try:
        # r = http_bridge.get(metadata_url)
        r = requests.get(metadata_url)
    except Exception:
        raise DatasetError("No such dataset in dClimate")
    # finally:
        # http_bridge.close()
    r.raise_for_status()
    return tupleify(r.json())

def get_gridcell_ipfs(
        lat,
        lon,
        dataset,
        also_return_snapped_coordinates=False,
        also_return_metadata=False,
        use_imperial_units=True,
        convert_to_local_time=True,
        as_of=None,
        ipfs_timeout=None):
    """
    Get the historical timeseries data for a gridded dataset in a dictionary
    This is a dictionary of dates/datetimes: climate values for a given dataset and
    lat, lon.
    also_return_metadata is set to False by default, but if set to True,
    returns the metadata next to the dict within a tuple.
    use_imperial_units is set to True by default, but if set to False,
    will get the appropriate metric unit from aliases_and_units
    """
    try:
        metadata = get_metadata_http(get_heads()[dataset])[0]
    except KeyError:
        raise DatasetError("No such dataset in dClimate")

    # set up units
    str_u = metadata['unit of measurement']
    with u.imperial.enable():
        dweather_unit = UNIT_ALIASES[str_u] if str_u in UNIT_ALIASES else u.Unit(str_u)
    converter = None

    # if imperial is desired and dweather_unit is metric
    if use_imperial_units and (dweather_unit in M2I):
        converter = M2I[dweather_unit]
    # if metric is desired and dweather_unit is imperial
    elif (not use_imperial_units) and (dweather_unit in I2M):
        converter = I2M[dweather_unit]

    # get dataset-specific "no observation" value
    missing_value = metadata["missing value"]

    try:
        dataset_obj = GRIDDED_DATASETS[dataset](as_of=as_of, ipfs_timeout=ipfs_timeout)
    except KeyError:
        raise DatasetError("No such dataset in dClimate")

    try:
        (lat, lon), resp_series = dataset_obj.get_data(lat, lon)

    except (ipfshttpclient.exceptions.ErrorResponse, ipfshttpclient.exceptions.TimeoutError, KeyError, FileNotFoundError) as e:
        raise CoordinateNotFoundError("Invalid coordinate for dataset")

    # try a timezone-based transformation on the times in case we're using an hourly set.
    if convert_to_local_time:
        try:
            tf = TimezoneFinder()
            local_tz = pytz.timezone(tf.timezone_at(lng=lon, lat=lat))
            resp_series = resp_series.tz_localize("UTC").tz_convert(local_tz)
        except (AttributeError, TypeError):  # datetime.date (daily sets) doesn't work with this, only datetime.datetime (hourly sets)
            pass

    if type(missing_value) == str:
        resp_series = resp_series.replace(missing_value, np.NaN).astype(float)
    else:
        resp_series.loc[resp_series.astype(float) == missing_value] = np.NaN
        resp_series = resp_series.astype(float)

    resp_series = resp_series * dweather_unit
    if converter is not None:
        resp_series = pd.Series(converter(resp_series.values), resp_series.index)
    result = tupleify({k: convert_nans_to_none(v) for k, v in resp_series.to_dict().items()})

    if also_return_metadata:
        result = tupleify(result) + ({"metadata": metadata},)
    if also_return_snapped_coordinates:
        result = tupleify(result) + ({"snapped to": (lat, lon)},)
    return result
