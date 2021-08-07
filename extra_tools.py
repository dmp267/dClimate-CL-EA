import ipfshttpclient, json, datetime, os, tarfile, gzip, pickle, zipfile
from abc import ABC, abstractmethod
from astropy import units as u
from astropy.units import imperial
import requests, json
from io import BytesIO
from collections import Counter, deque

GATEWAY_URL = 'https://gateway.arbolmarket.com'

METADATA_FILE = "metadata.json"

UNIT_ALIASES = {
    "kg/m**2": u.kg / u.m**2,
    "mm": u.mm,
    "degC": u.deg_C,
    "m s**-1": u.m / u.s,
    "degF": imperial.deg_F,
    "m of water equivalent": u.m
}

METRIC_TO_IMPERIAL = {
    u.m: lambda q: q.to(imperial.inch),
    u.mm: lambda q: q.to(imperial.inch),
    u.deg_C: lambda q: q.to(imperial.deg_F, equivalencies=u.temperature()),
    u.K: lambda q: q.to(imperial.deg_F, equivalencies=u.temperature()),
    u.kg / u.m**2: lambda q: q.to(imperial.pound / imperial.ft ** 2),
    u.m / u.s: lambda q: q.to(imperial.mile / u.hour)
}

IMPERIAL_TO_METRIC = {
    imperial.inch: lambda q: q.to(u.mm),
    imperial.deg_F: lambda q: q.to(u.deg_C, equivalencies=u.temperature()),
    imperial.pound / imperial.ft ** 2: lambda q: q.to(u.kg / u.m**2),
    imperial.mile / u.hour: lambda q: q.to(u.m / u.s)
}

class IPFSError(Exception):
    """Base class for exceptions in this module."""
    pass

class DatasetError(IPFSError):
    """Excpetion raised when a dataset cannot be found on ipfs"""
    pass

class CoordinateNotFoundError(IPFSError):
    """Exception raised when a lat/lon coordinate pair does not have a file on the server"""
    pass

class IpfsDataset(ABC):
    """
    Base class for handling requests for all IPFS datasets
    """
    @property
    @abstractmethod
    def dataset(self):
        """
        Dataset name must be overwritten by leaf class in order to be instantiable
        """
        pass

    def __init__(self, as_of=None, ipfs_timeout=None):
        """
        args:
        :ipfs_timeout: Time IPFS should wait for response before throwing exception. If None, will assume that
        code is running in an environment containing all datasets (such as gateway)
        """
        self.on_gateway = not ipfs_timeout
        self.ipfs = ipfshttpclient.connect(timeout=ipfs_timeout)
        self.as_of = as_of

    def get_metadata(self, h):
        """
        args:
        :h: dClimate IPFS hash from which to get metadata
        return:
            metadata as dict
        """
        if not self.on_gateway:
            self.ipfs._client.request('/swarm/connect', (GATEWAY_IPFS_ID,))
        metadata = self.ipfs.cat(f"{h}/{METADATA_FILE}").decode('utf-8')
        return json.loads(metadata)

    def get_file_object(self, f):
        """
        args:
        :h: dClimate IPFS hash from which to get data. Must point to a file, not a directory
        return:
            content of file as file-like bytes object
        """
        if not self.on_gateway:
            self.ipfs._client.request('/swarm/connect', (GATEWAY_IPFS_ID,))
        return BytesIO(self.ipfs.cat(f))

    def traverse_ll(self, head, as_of=None):
        """
        Iterates through a linked list of metadata files
        args:
        :head: ipfs hash of the directory at the head of the linked list
        return: deque containing all hashes in the linked list
        """
        release_itr = head
        release_ll = deque()
        while True:
            metadata = self.get_metadata(release_itr)
            if as_of:
                date_generated = datetime.datetime.fromisoformat(metadata["time generated"])
                if date_generated <= as_of:
                    release_ll.appendleft(release_itr)
            else:
                release_ll.appendleft(release_itr)
            try:
                prev_release = metadata['previous hash']
            except KeyError:
                return release_ll
            if prev_release is not None:
                release_itr = prev_release
            else:
                return release_ll

    @abstractmethod
    def get_data(self, *args, **kwargs):
        """
        Exposed method that allows user to get data in the dataset. Args and return value will depend on whether
        this is a gridded, station or storm dataset
        """
        self.head = get_heads()[self.dataset]

class GriddedDataset(IpfsDataset):
    """
    Abstract class from which all gridded, linked list datasets inherit
    """
    @classmethod
    def snap_to_grid(cls, lat, lon, metadata):
        """
        Find the nearest (lat,lon) on IPFS for a given metadata file.
        args:
        :lat: = -90 < lat < 90, float
        :lon: = -180 < lon < 180, float
        :metadata: a dWeather metadata file
        return: lat, lon
        """
        resolution = metadata['resolution']
        min_lat = metadata['latitude range'][0]  # start [lat, lon]
        min_lon = metadata['longitude range'][0]  # end [lat, lon]

        # check that the lat lon is in the bounding box
        snap_lat = round(round((lat - min_lat) / resolution) * resolution + min_lat, 3)
        snap_lon = round(round((lon - min_lon) / resolution) * resolution + min_lon, 3)
        return snap_lat, snap_lon

    def get_hashes(self):
        """
        return: list of all hashes in dataset
        """
        hashes = self.traverse_ll(self.head, self.as_of)
        return list(hashes)

    def get_date_range_from_metadata(self, h):
        """
        args:
        :h: hash for ipfs directory containing metadata
        return: list of [start_time, end_time]
        """
        metadata = self.get_metadata(h)
        str_dates = (metadata["date range"][0], metadata["date range"][1])
        return [datetime.datetime.fromisoformat(dt) for dt in str_dates]

    def get_weather_dict(self, date_range, ipfs_hash, is_root):
        """
        Get a pd.Series of weather values for a given IPFS hash
        args:
        :date_range: time range that hash has data for
        :ipfs_hash: hash containing data
        :is_root: bool indicating whether this is the root node in the linked list
        return: pd.Series with date or datetime index and weather values
        """
        if not is_root:
            try:
                with tarfile.open(fileobj=self.get_file_object(f"{ipfs_hash}/{self.tar_name}")) as tar:
                    member = tar.getmember(self.gzip_name)
                    with gzip.open(tar.extractfile(member)) as gz:
                        cell_text = gz.read().decode('utf-8')
            except ipfshttpclient.exceptions.ErrorResponse:
                zip_file_name = self.tar_name[:-4] + '.zip'
                with zipfile.ZipFile(self.get_file_object(f"{ipfs_hash}/{zip_file_name}")) as zi:
                    with gzip.open(zi.open(self.gzip_name)) as gz:
                        cell_text = gz.read().decode('utf-8')
        else:
            with gzip.open(self.get_file_object(f"{ipfs_hash}/{self.gzip_name}")) as gz:
                cell_text = gz.read().decode('utf-8')

        day_itr = date_range[0]
        weather_dict = {}
        if "daily" in self.dataset:
            for year_data in cell_text.split('\n'):
                for day_data in year_data.split(','):
                    weather_dict[day_itr.date()] = day_data
                    day_itr = day_itr + datetime.timedelta(days=1)
        elif "hourly" in self.dataset:
            for year_data in cell_text.split('\n'):
                for hour_data in year_data.split(','):
                    weather_dict[day_itr] = hour_data
                    day_itr = day_itr + datetime.timedelta(hours=1)
        return weather_dict

class SimpleGriddedDataset(GriddedDataset):
    """
    Abstract class for all other gridded datasets
    """
    SIG_DIGITS = 3

    @property
    def zero_padding(self):
        """
        Constant for how file names are formatted
        """
        return None

    def get_file_names(self):
        """
        Uses formatting and lat,lon to determine file name containing data
        return: dict with names for tar and gz versions of file
        """
        if self.zero_padding:
            lat_portion = f"{self.snapped_lat:0{self.zero_padding}.{self.SIG_DIGITS}f}"
            lon_portion = f"{self.snapped_lon:0{self.zero_padding}.{self.SIG_DIGITS}f}"
        else:
            lat_portion = f"{self.snapped_lat:.{self.SIG_DIGITS}f}"
            lon_portion = f"{self.snapped_lon:.{self.SIG_DIGITS}f}"
        return {
            "tar": f"{lat_portion}.tar",
            "gz": f"{lat_portion}_{lon_portion}.gz"
        }

    def get_data(self, lat, lon):
        """
        General method for gridded datasets getting data
        args:
        :lat: float of latitude from which to get data
        :lon: float of longitude from which to get data
        return: tuple of lat/lon snapped to dataset grid, and weather data, is pd.Series with datetime or date index
        and str values corresponding to weather observations
        """
        super().get_data()
        first_metadata = self.get_metadata(self.head)
        if "cpcc" in self.dataset or "era5" in self.dataset:
            lat, lon = conventional_lat_lon_to_cpc(float(lat), float(lon))
        self.snapped_lat, self.snapped_lon = self.snap_to_grid(float(lat), float(lon), first_metadata)
        self.tar_name = self.get_file_names()["tar"]
        self.gzip_name = self.get_file_names()["gz"]
        ret_dict = {}
        for i, h in enumerate(self.get_hashes()):
            date_range = self.get_date_range_from_metadata(h)
            weather_dict = self.get_weather_dict(date_range, h, i == 0)
            ret_dict = {**ret_dict, **weather_dict}
        ret_lat, ret_lon = cpc_lat_lon_to_conventional(self.snapped_lat, self.snapped_lon)
        return (float(ret_lat), float(ret_lon)), pd.Series(ret_dict)

def get_heads(url=GATEWAY_URL):
    """
    Get heads.json for a given IPFS gateway.
    Args:
        url (str): base url of the IPFS gateway url
    Returns (example heads.json):
        {
            'chirps_05-daily': 'Qm...',
            'chirps_05-monthly': 'Qm...',
            'chirps_25-daily': 'Qm...',
            'chirps_25-monthly': 'Qm...',
            'cpc_us-daily': 'Qm...',
            'cpc_us-monthly': 'Qm...'
        }
    """
    hashes_url = url + "/climate/hashes/heads.json"
    r = requests.get(hashes_url)
    r.raise_for_status()
    return r.json()

def convert_nans_to_none(quantity):
    if np.isnan(quantity.value):
        return None
    else:
        return quantity

def tupleify(args):
    if isinstance(args, tuple):
        return args
    return (args,)

def cpc_lat_lon_to_conventional(lat, lon):
    """
    Convert a pair of coordinates from the idiosyncratic CPC lat lon
    format to the conventional lat lon format.
    """
    lat, lon = float(lat), float(lon)
    if (lon >= 180):
        return lat, lon - 360
    else:
        return lat, lon

def conventional_lat_lon_to_cpc(lat, lon):
    """
    Convert a pair of coordinates from conventional (lat,lon)
    to the idiosyncratic CPC (lat,lon) format.
    """
    lat, lon = float(lat), float(lon)
    if (lon < 0):
        return lat, lon + 360
    else:
        return lat, lon
