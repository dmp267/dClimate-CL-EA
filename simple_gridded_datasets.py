from extra_tools import get_heads, METADATA_FILE, IpfsDataset, GriddedDataset, SimpleGriddedDataset

class CpccPrecipUsDaily(SimpleGriddedDataset):
    dataset = "cpcc_precip_us-daily"

class CpccPrecipGlobalDaily(SimpleGriddedDataset):
    dataset = "cpcc_precip_global-daily"

class CpccTempMaxDaily(SimpleGriddedDataset):
    dataset = "cpcc_temp_max-daily"

class CpccTempMinDaily(SimpleGriddedDataset):
    dataset = "cpcc_temp_min-daily"

class ChirpscFinal05Daily(SimpleGriddedDataset):
    dataset = "chirpsc_final_05-daily"

class ChirpscFinal25Daily(SimpleGriddedDataset):
    dataset = "chirpsc_final_25-daily"

class ChirpscPrelim05Daily(SimpleGriddedDataset):
    dataset = "chirpsc_prelim_05-daily"

class Era5Land2mTempHourly(SimpleGriddedDataset):
    dataset = "era5_land_2m_temp-hourly"

class Era5LandPrecipHourly(SimpleGriddedDataset):
    dataset = "era5_land_precip-hourly"

class Era5LandSurfaceSolarRadiationDownwardsHourly(SimpleGriddedDataset):
    dataset = "era5_land_surface_solar_radiation_downwards-hourly"

class Era5LandSnowfallHourly(SimpleGriddedDataset):
    dataset = "era5_land_snowfall-hourly"

class Era5SurfaceRunoffHourly(SimpleGriddedDataset):
    dataset = "era5_surface_runoff-hourly"

class Era5Wind100mUHourly(SimpleGriddedDataset):
    dataset = "era5_wind_100m_u-hourly"

class Era5Wind100mVHourly(SimpleGriddedDataset):
    dataset = "era5_wind_100m_v-hourly"

class Era5VolumetricSoilWater(SimpleGriddedDataset):
    dataset = "era5_volumetric_soil_water_layer_1-hourly"
