# coding: utf-8
import pysqlite2.dbapi2 as sqlite
from array import array
import math
import zlib

TILE_PARTS_N = 4
POINTS_IN_TILE = 1200 / TILE_PARTS_N


class SqliteStorage(object):
    def __init__(self, path):
        self.conn = sqlite.connect(path)

    def get_tile(self, lat, lon, tile_n):
        data = self.conn.execute('SELECT tile_data FROM dem_tiles WHERE lat=? AND lon=? AND tile_n=?',
                                 (lat, lon, tile_n)).fetchone()
        if data is None:
            return None
        data = str(data[0])
        data = zlib.decompress(data)
        data = array('h', data)
        return data


def tile_index_for_point(lat, lon):
    half_cell = 1. / 1200 / 2
    lat += half_cell
    lon += half_cell
    tile_lat = int(math.floor(lat))
    tile_lon = int(math.floor(lon))
    frac_lon = lon - tile_lon
    frac_lat = lat - tile_lat
    col = int(frac_lon * 1200.)
    row = int(frac_lat * 1200.)
    if row == 0:
        row = 1199
        tile_lat -= 1
    tile_x = col / POINTS_IN_TILE
    x = col - tile_x * POINTS_IN_TILE
    tile_y = row / POINTS_IN_TILE
    y = row - tile_y * POINTS_IN_TILE
    tile_y = TILE_PARTS_N - tile_y - 1
    y = POINTS_IN_TILE - y - 1
    return (tile_lat, tile_lon, tile_y * TILE_PARTS_N + tile_x), (x, y)


def get_elevations(latlons, db_path):
    elevations = []
    storage = SqliteStorage(db_path)
    tile = None
    cur_tile_index = None
    for lat, lon in latlons:
        tile_ind, (x, y) = tile_index_for_point(lat, lon)
        if tile_ind != cur_tile_index:
            tile = storage.get_tile(*tile_ind)
            cur_tile_index = tile_ind
        if tile is None:
            elevations.append(None)
        else:
            elevations.append(tile[x + y * POINTS_IN_TILE])
    return elevations

if __name__ == '__main__':
    db_path = '/home/w/tmp/dem_tiles'
    print get_elevations([(39.7781889474084, 2.82159449205255)], db_path)