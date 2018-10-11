# coding: utf-8
import sqlite3 as sqlite
from array import array
import math
import zlib


TILE_PARTS_N = 4
POINTS_IN_TILE = 1200 / TILE_PARTS_N


class SqliteStorage(object):
    cache_size = 4

    def __init__(self, path):
        self.conn = sqlite.connect(path)
        self._cache = []

    def get_tile(self, lat, lon, tile_n):
        for item in self._cache:
            if item[0] == (lat, lon, tile_n):
                self._cache.remove(item)
                break
        else:
            item = ((lat, lon, tile_n), self._get_tile(lat, lon, tile_n))
        self._cache.insert(0, item)
        del self._cache[self.cache_size:]
        return item[1]

    def _get_tile(self, lat, lon, tile_n):
        data = self.conn.execute('SELECT tile_data FROM dem_tiles WHERE lat=? AND lon=? AND tile_n=?',
                                 (lat, lon, tile_n)).fetchone()
        if data is None:
            return None
        data = str(data[0])
        data = zlib.decompress(data)
        data = array('h', data)
        return data


def tile_index_for_point(lat, lon):
    tile_lat = int(math.floor(lat))
    tile_lon = int(math.floor(lon))
    frac_lon = lon - tile_lon
    frac_lat = lat - tile_lat
    col = int(math.floor(frac_lon * 1200.))
    row = int(math.ceil(frac_lat * 1200.))
    row = 1200 - row
    if row == 1200:
        row = 0
        tile_lat -= 1
    tile_x = col / POINTS_IN_TILE
    x = col - tile_x * POINTS_IN_TILE
    tile_y = row / POINTS_IN_TILE
    y = row - tile_y * POINTS_IN_TILE
    tile_lon2 = tile_lon
    tile_x2 = tile_x
    x2 = x + 1
    if x2 >= POINTS_IN_TILE:
        x2 = 0
        tile_x2 += 1
    if tile_x2 == TILE_PARTS_N:
        tile_x2 = 0
        tile_lon2 += 1
    y2 = y + 1
    tile_y2 = tile_y
    if y2 >= POINTS_IN_TILE:
        y2 = 0
        tile_y2 += 1
    tile_lat2 = tile_lat
    if tile_y2 == TILE_PARTS_N:
        tile_y2 = 0
        tile_lat2 -= 1
    bilinear_indexes = [
        ((tile_lat, tile_lon, tile_y * TILE_PARTS_N + tile_x), (x, y)),
        ((tile_lat, tile_lon2, tile_y * TILE_PARTS_N + tile_x2), (x2, y)),
        ((tile_lat2, tile_lon, tile_y2 * TILE_PARTS_N + tile_x), (x, y2)),
        ((tile_lat2, tile_lon2, tile_y2 * TILE_PARTS_N + tile_x2), (x2, y2)),
    ]
    dx = frac_lon * 1200 - col
    dy = 1200 - frac_lat * 1200 - row
    if dy == 1200:
        dy = 0
    assert 0 <= dx <= 1, dx
    assert 0 <= dy <= 1, dy

    return bilinear_indexes, (dx, dy)


def get_elevations(latlons, db_path):
    elevations = []
    storage = SqliteStorage(db_path)
    tile = None
    cur_tile_index = None
    for lat, lon in latlons:
        bilinear_values = []
        bilinear_indexes, bilinear_offset = tile_index_for_point(lat, lon)
        for tile_ind, (x, y) in bilinear_indexes:
            if tile_ind != cur_tile_index:
                tile = storage.get_tile(*tile_ind)
                cur_tile_index = tile_ind
            if tile is None:
                elevations.append(None)
                break
            value = tile[x + y * POINTS_IN_TILE]
            if value == -32768:
                elevations.append(None)
                break
            bilinear_values.append(value)
        else:
            # print bilinear_values
            elevation = (bilinear_values[0] * (1 - bilinear_offset[0]) * (1 - bilinear_offset[1]) +
                         bilinear_values[1] * (bilinear_offset[0]) * (1 - bilinear_offset[1]) +
                         bilinear_values[2] * (1 - bilinear_offset[0]) * (bilinear_offset[1]) +
                         bilinear_values[3] * (bilinear_offset[0]) * (bilinear_offset[1]) )
            elevations.append(elevation)
    return elevations

if __name__ == '__main__':
    db_path = '/home/w/tmp/dem_tiles'
    # print get_elevations([(39.7781889474084, 2.82159449205255)], db_path)
    print get_elevations([(55.000000+0.0000001, 58.96684)], db_path)
    # print get_elevations([(55.100000, 59.000000-.000001)], db_path)
    # cell = 1. / 1200
    # print tile_index_for_point(35 + cell * .49, 50)