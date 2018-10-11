# coding: utf-8
from array import array
import re
import math
import os
import zlib
import progressbar
import sqlite3 as sqlite
import mpimap



class FileStorage(object):
    def __init__(self, path):
        self.base_dir = path
        if os.path.exists(path):
            raise ValueError('Path "%s" already exists' % path)
        os.makedirs(path)

    def put(self, index, data):
        index = map(str, index)
        filename = '_'.join(index)
        filename = os.path.join(self.base_dir, filename)
        with open(filename, 'w') as f:
            f.write(data)

    def close(self):
        pass


class SqliteStorage(object):
    SCHEME = '''
        CREATE TABLE dem_tiles(
            lat integer, lon integer, tile_n integer, tile_data blob, UNIQUE(lat, lon, tile_n));
    '''

    PRAGMAS = '''
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = 0;
    '''

    @property
    def conn(self):
        conn = sqlite.connect(self.path)
        conn.executescript(self.PRAGMAS)
        return conn

    def __init__(self, path):
        if os.path.exists(path):
            raise ValueError('Path "%s" already exists' % path)
        self.path = path
        self.conn.executescript(self.SCHEME)

    def put(self, index, data):
        conn = self.conn
        conn.execute('''
            INSERT INTO dem_tiles (lat, lon, tile_n, tile_data) VALUES (?,?,?,?)''',
                     index + (buffer(data),))
        conn.commit()
        conn.close()

    def close(self):
        self.conn.execute('PRAGMA journal_mode = off')



def dem_encode_gzip(data):
    return zlib.compress(data.tostring(), 8)


def read_hgt_file(f):
    s = f.read()
    ar = array('h', s)
    del ar[-1201:]
    del ar[1200::1201]
    ar.byteswap()
    return ar


def hgt_index(hgt_name):
    m = re.match(r'([NS])(\d{2})([EW])(\d{3})\.HGT', hgt_name.upper())
    lat = int(m.group(2))
    lon = int(m.group(4))
    if m.group(1) == 'S':
        lat = -lat
    if m.group(3) == 'W':
        lon = -lon
    return lat, lon


def split_dem(data):
    parts_n = 4
    tile_size = 1200 / parts_n
    assert tile_size * parts_n == 1200
    assert len(data) == 1200 * 1200
    for tile_y in xrange(parts_n):
        for tile_x in xrange(parts_n):
            tile = array(data.typecode)
            for row in xrange(tile_size):
                i = tile_y * 1200 * tile_size + tile_x * tile_size + row * 1200
                tile.extend(data[i:i + tile_size])
            assert len(tile) == tile_size * tile_size
            yield tile


def build_tile(hgt_name, hgt_dir, storage):
    ind = hgt_index(hgt_name)
    with open(os.path.join(hgt_dir, hgt_name)) as f:
        dem = read_hgt_file(f)
    for tile_index, tile in enumerate(split_dem(dem)):
        dem_encoded = dem_encode_gzip(tile)
        storage.put(ind + (tile_index,), dem_encoded)


def build_tiles(hgt_dir, storage_dir):
    hgt_names = (os.listdir(hgt_dir))

    # import random
    # random.seed(1)
    # hgt_names = random.sample(hgt_names, 100)

    storage = SqliteStorage(storage_dir)
    progress = progressbar.ProgressBar(widgets=[progressbar.Bar(), progressbar.Percentage(), " ", progressbar.Timer(),
                                                " ", progressbar.ETA()])

    progress.maxval = len(hgt_names)
    progress.start()
    for i, hgt_name in enumerate(mpimap.mpimap(build_tile, hgt_names, hgt_dir=hgt_dir, storage=storage)):
        # build_tile(hgt_name, hgt_dir, storage)
        progress.update(i)
    storage.close()


if __name__ == '__main__':
    import time

    hgt_dir = '/mm/geo/dem/viewfinderpanoramas-3/uncompressed/'
    # hgt_dir = '/home/w/tmp/test_dems'
    test_path = '/home/w/tmp/dem_tiles.db'
    t = time.time()
    build_tiles(hgt_dir, test_path)
    print time.time() - t
    # with open(hgt_file) as f:
    #     x = read_hgt_file(f.read())
    # print lon_to_tile_x(37.75658, 12)
    # print lat_to_tile_y(85.051, 12)
