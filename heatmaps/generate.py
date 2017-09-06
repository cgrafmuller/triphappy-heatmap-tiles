# coding=utf-8
# Generates heatmaps & uploads them to the S3 Tile Server
# Uses Numpy for basic numerical operations
# Uses SKLearn (Scipy) for DBSCAN clustering
# Uses Amazon's Boto3 SDK for interacting with S3 & our RDS database
# Heatmaps generated with the lucasb-eyer heatmap C library found here: https://github.com/lucasb-eyer/heatmap
# Made with love by Carl Grafmuller 😘 (https://github.com/cgrafmuller)

import sys
import heatmap_c as heatmap
import psycopg2
import rds_config
import math
import boto3
import os
import numpy as np
from sklearn.cluster import DBSCAN
import threading

# Amazon info - should probably remove them from Git
rds_host = rds_config.host
user_name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name
bucket = rds_config.bucket
s3_client = boto3.client('s3')

# Global parameters:
# Minimum/maximum zoom levels to run & the step size to use when generating
# Future feature: run at one zoom level (e.g. 11) and use that same heatmap for the next level (12)
# Radius is the distance to grab all venues around the city's lat,lng center
# Tile resolution is the number of pixels in a heatmap tile (set by what Google/OSM/Bing uses)
# Zoom parameters are for adjusting heatmap dotsize & clustering parameters by zoom level

MIN_ZOOM = 10  # 10
MAX_ZOOM = 16  # 16
ZOOM_STEP = 1
RADIUS = 17500
TILE_RESOLUTION = 256
ZOOM_PARAMETERS = {
    10: {'dotsize': 10, 'opacity': 1, 'cluster': True, 'cluster_epsilon': 0.5, 'cluster_neighbors': 4},
    11: {'dotsize': 10, 'opacity': 1, 'cluster': True, 'cluster_epsilon': 0.5, 'cluster_neighbors': 4},
    12: {'dotsize': 15, 'opacity': 1, 'cluster': True, 'cluster_epsilon': 0.5, 'cluster_neighbors': 4},
    13: {'dotsize': 15, 'opacity': 1, 'cluster': True, 'cluster_epsilon': 0.65, 'cluster_neighbors': 4},
    14: {'dotsize': 20, 'opacity': 1, 'cluster': True, 'cluster_epsilon': 0.65, 'cluster_neighbors': 4},
    15: {'dotsize': 40, 'opacity': 1, 'cluster': True, 'cluster_epsilon': 0.8, 'cluster_neighbors': 4},
    16: {'dotsize': 60, 'opacity': 1, 'cluster': True, 'cluster_epsilon': 0.8, 'cluster_neighbors': 4},
    17: {'dotsize': 80, 'opacity': 1, 'cluster': False, 'cluster_epsilon': 1.0, 'cluster_neighbors': 1},
    18: {'dotsize': 100, 'opacity': 1, 'cluster': False, 'cluster_epsilon': 1.0, 'cluster_neighbors': 2}
}
# Enum for run methods of running a whole city
RUN_BY_CATEGORY = 1
RUN_BY_ZOOM = 2


# Takes in a (lat, lng) coordinate pair and returns the 3857 Pseudo-Mercator point coordinates (x, y) at the requested zoom level
def convert_coords_to_mercator_point(lat, lng, zoom):
    mapWidth = 2**zoom
    mapHeight = 2**zoom

    x = (lng + 180) * (mapWidth / 360.0)

    latRad = lat * (math.pi / 180)
    mercN = math.log(math.tan((latRad / 2) + (math.pi / 4)))
    y = (mapHeight / 2) - (mercN * mapWidth / (2 * math.pi))

    return (x, y)


# Takes in a (lat, lng) coordinate pair and returns the 3857 Pseudo-Mercator point coordinates (x, y) of the NW corner of the map tile at the requested zoom level
def convert_coords_to_tile(lat, lng, zoom):
    point = convert_coords_to_mercator_point(lat, lng, zoom)
    # Flooring the x, y values moves the point to the NW corner of the tile
    temp = np.floor(point).astype(int)
    # Return a tuple for consistency
    return (temp[0], temp[1])


# Takes in an (x, y) coordinate pair in the 3857 Pseudo-Mercator projection at the requested zoom level an returns the (lat, lng) coordinates
def convert_point_to_coords(x, y, zoom):
    mapWidth = 2**zoom
    mapHeight = 2**zoom

    lng = (x * 360.0 / mapWidth) - 180

    mercN = ((mapHeight / 2) - y) * (2 * math.pi) / mapWidth
    latRad = (math.atan(math.exp(mercN)) - (math.pi / 4)) * 2
    lat = (latRad * 180) / math.pi

    return (lat, lng)


# Generates heatmaps for all categories & all zoom levels for a given lat, lng on a tile by tile basis
# Meaning, each tile is run by itself, grabbing data, clustering, generating
# Causes some clipping issues if there's a venue near the border of the tile since the other tiles don't know about each other
# Run_method is an integer flag to have it run either by category then zoom level or vice versa
def generate_all_heatmaps_by_tile(lat, lng, run_method=RUN_BY_ZOOM):
    if run_method == RUN_BY_CATEGORY:
        for cat in range(1, 5):
            for z in range(MIN_ZOOM, MAX_ZOOM + 1, ZOOM_STEP):
                generate_heatmap_by_tile(cat, z, lat, lng)
    elif run_method == RUN_BY_ZOOM:
        for z in range(MIN_ZOOM, MAX_ZOOM + 1, ZOOM_STEP):
            for cat in range(1, 5):
                generate_heatmap_by_tile(cat, z, lat, lng)
    else:
        raise ValueError('Invalid run_method specified. Must be either 1 or 2.')


# Generates heatmaps for all categories & all zoom levels for a given lat, lng on a citywide basis
# Meaning, the entire city is run as one BIG heatmap & then cut up into tiles
# Fixes the clipping issues above, but can take a while to generate for big cities due to the sheer size of the citywide heatmap
def generate_all_heatmaps_by_city(lat, lng, run_method=RUN_BY_ZOOM):
    if run_method == RUN_BY_CATEGORY:
        for cat in range(1, 5):
            for z in range(MIN_ZOOM, MAX_ZOOM + 1, ZOOM_STEP):
                new_radius = RADIUS
                # If it crashes due to memory/overflow issues, then run with a smaller radius
                try:
                    generate_heatmap_by_city(cat, z, lat, lng, new_radius)
                except:
                    generate_heatmap_by_city(cat, z, lat, lng, new_radius - 2500)
    elif run_method == RUN_BY_ZOOM:
        for z in range(MIN_ZOOM, MAX_ZOOM + 1, ZOOM_STEP):
            for cat in range(1, 5):
                new_radius = RADIUS
                # If it crashes due to memory/overflow issues, then run with a smaller radius
                try:
                    generate_heatmap_by_city(cat, z, lat, lng, new_radius)
                except:
                    generate_heatmap_by_city(cat, z, lat, lng, new_radius - 2500)
    else:
        raise ValueError('Invalid run_method specified. Must be either 1 or 2.')


# Generates a heatmap for a given category for a given lat, lng on a tile by tile basis
def generate_heatmap_by_tile(category, zoom, lat, lng):
    print "Running category: {0} at zoom: {1}".format(category, zoom)

    # Grab all venues in the category within RADIUS distance of the (lat, lng) coordinates
    pts = []
    with conn.cursor() as cur:
        query = "SELECT lat, lng FROM heatmap_venues WHERE category_id='{0}' AND ST_DWithin(geog_point, ST_GeographyFromText('SRID=4326;POINT({1} {2})'), {3});".format(category, lng, lat, RADIUS)
        cur.execute(query)
        for row in cur:
            # Stored as (lat,lng)
            pts.append((float(row[0]), float(row[1])))

    print "Processing %d points..." % len(pts)

    # We should only calculate the tiles that have venues in them
    tiles = []
    for venue in pts:
        # Find the tile for each venue & add it to the list
        tile = convert_coords_to_tile(venue[0], venue[1], zoom)
        if tile not in tiles:
            tiles.append(tile)
    # Sorted for easy debugging
    tiles = sorted(tiles)

    # Now run all dem tiles
    for tile in tiles:
        generate_tile(category, zoom, tile[0], tile[1])


# Generates heatmap on a whole city basis then cuts it into tiles.
# This is in contrast to generate_heatmap which clusters & generates each tile by itself.
def generate_heatmap_by_city(category, zoom, lat, lng, radius):
    print "Running category: {0} at zoom: {1}".format(category, zoom)

    # Grab all venues in the category within RADIUS distance of the (lat, lng) coordinates
    pts = []
    with conn.cursor() as cur:
        query = "SELECT lat, lng FROM heatmap_venues WHERE category_id='{0}' AND ST_DWithin(geog_point, ST_GeographyFromText('SRID=4326;POINT({1} {2})'), {3});".format(category, lng, lat, radius)
        cur.execute(query)
        for row in cur:
            # REMEMBER: x = lng, y = lat
            # Stored as (lng, lat)
            pts.append((float(row[1]), float(row[0])))

    total_venues = len(pts)
    print "Processing %d venues..." % total_venues

    # If there's venues
    if total_venues > 0:
        # If we want to cluster, cluster
        if ZOOM_PARAMETERS[zoom]['cluster']:
            kms_per_radian = 6371.0088
            epsilon = ZOOM_PARAMETERS[zoom]['cluster_epsilon'] / kms_per_radian
            db = DBSCAN(eps=epsilon, min_samples=ZOOM_PARAMETERS[zoom]['cluster_neighbors'], algorithm='ball_tree', metric='haversine').fit(np.radians(np.matrix(pts)))
            for idx, val in reversed(list(enumerate(db.labels_))):
                # Remove all the non-clustered points that reside in the [-1] index
                if val == -1:
                    # Stored as (lng, lat)
                    pts.remove((pts[idx][0], pts[idx][1]))

            print "Clustered %d venues into %d points at zoom level %d" % (total_venues, len(pts), zoom)

        # If we're clustering, only proceed if we now have points
        if len(pts) > 0:
            # We should only calculate the tiles that have venues in them
            tiles = []
            for venue in pts:
                # Find the tile for each venue & add it to the list
                tile = convert_coords_to_tile(venue[1], venue[0], zoom)
                if tile not in tiles:
                    tiles.append(tile)
                # Check if the venue is close to the edge of the tile and add in any relevant neighbors to prevent clipping
                x = tile[0]
                y = tile[1]
                # Get lat, lng of SW corner (y + 1)
                min_coords = convert_point_to_coords(x, y + 1, zoom)
                # Get lat, lng of NE corner (x + 1)
                max_coords = convert_point_to_coords(x + 1, y, zoom)
                # Normalize venue's pixel coordinates for the tile
                pixel_y = ((max_coords[0] - venue[1]) / (max_coords[0] - min_coords[0])) * (TILE_RESOLUTION - 1)
                pixel_x = ((venue[0] - min_coords[1]) / (max_coords[1] - min_coords[1])) * (TILE_RESOLUTION - 1)

                # Check if the pixel coordinates are near the boundary:
                # Clipping on the left!
                if pixel_x < ZOOM_PARAMETERS[zoom]['dotsize']:
                    neighbor_tile = (x - 1, y)
                    if neighbor_tile not in tiles:
                        tiles.append(neighbor_tile)
                    # Clipping on the top left!
                    if pixel_y < ZOOM_PARAMETERS[zoom]['dotsize']:
                        neighbor_tile = (x - 1, y - 1)
                        if neighbor_tile not in tiles:
                            tiles.append(neighbor_tile)
                    # Clipping on the bottom left!
                    if (TILE_RESOLUTION - pixel_y) < ZOOM_PARAMETERS[zoom]['dotsize']:
                        neighbor_tile = (x - 1, y + 1)
                        if neighbor_tile not in tiles:
                            tiles.append(neighbor_tile)
                # Clipping on the top!
                if pixel_y < ZOOM_PARAMETERS[zoom]['dotsize']:
                    neighbor_tile = (x, y - 1)
                    if neighbor_tile not in tiles:
                        tiles.append(neighbor_tile)
                # Clipping on the right!
                if (TILE_RESOLUTION - pixel_x) < ZOOM_PARAMETERS[zoom]['dotsize']:
                    neighbor_tile = (x + 1, y)
                    if neighbor_tile not in tiles:
                        tiles.append(neighbor_tile)
                    # Clipping on the top right!
                    if pixel_y < ZOOM_PARAMETERS[zoom]['dotsize']:
                        neighbor_tile = (x + 1, y - 1)
                        if neighbor_tile not in tiles:
                            tiles.append(neighbor_tile)
                    # Clipping on the bottom right!
                    if (TILE_RESOLUTION - pixel_y) < ZOOM_PARAMETERS[zoom]['dotsize']:
                        neighbor_tile = (x + 1, y + 1)
                        if neighbor_tile not in tiles:
                            tiles.append(neighbor_tile)
                # Clipping on the bottom!
                if (TILE_RESOLUTION - pixel_y) < ZOOM_PARAMETERS[zoom]['dotsize']:
                    neighbor_tile = (x, y + 1)
                    if neighbor_tile not in tiles:
                        tiles.append(neighbor_tile)

            # Sorted for easy debugging
            tiles = sorted(tiles)

            # Calculate the smallest bounding box that will cover the whole city and get the NW & SE tiles
            nw_tile = (min(tiles, key=lambda t: t[0])[0], min(tiles, key=lambda t: t[1])[1])
            se_tile = (max(tiles, key=lambda t: t[0])[0], max(tiles, key=lambda t: t[1])[1])

            # Get the coordinates of this bounding box
            min_coords = convert_point_to_coords(nw_tile[0], se_tile[1] + 1, zoom)
            max_coords = convert_point_to_coords(se_tile[0] + 1, nw_tile[1], zoom)

            # Get the # of tiles needed to cover the bounding box
            num_x_tiles = se_tile[0] - nw_tile[0] + 1
            num_y_tiles = se_tile[1] - nw_tile[1] + 1

            # Get the resolution of the citywide heatmap
            resolution_x = TILE_RESOLUTION * num_x_tiles
            resolution_y = TILE_RESOLUTION * num_y_tiles

            # Normalize the lat, lng coordinates into the pixel dimensions of the heatmap - dependent on # tiles
            normalized_pts = []
            for pt in pts:
                lat = ((max_coords[0] - pt[1]) / (max_coords[0] - min_coords[0])) * (resolution_y - 1)
                lng = ((pt[0] - min_coords[1]) / (max_coords[1] - min_coords[1])) * (resolution_x - 1)
                normalized_pts.append((lng, lat))

            # Generate a heatmap!
            hm = heatmap.Heatmap()
            print "Generating heatmap..."
            img = hm.heatmap(normalized_pts, opacity=ZOOM_PARAMETERS[zoom]['opacity'], dotsize=ZOOM_PARAMETERS[zoom]['dotsize'], size=(resolution_x, resolution_y), color_scheme='th_classic')

            # Cut the heatmap into tiles
            print "Generated heatmap, cutting into %d tiles" % len(tiles)
            count = 1
            for i in range(0, num_x_tiles):
                for j in range(0, num_y_tiles):
                    x = nw_tile[0] + i
                    y = nw_tile[1] + j
                    # If this tile should be run, run it
                    if (x, y) in tiles:
                        file_name = "{0}_{1}_{2}_{3}.png".format(category, zoom, x, y)
                        # Choppy choppy
                        img.crop((TILE_RESOLUTION * i, TILE_RESOLUTION * j, TILE_RESOLUTION * (i + 1), TILE_RESOLUTION * (j + 1))).save(file_name, format="PNG")

                        # Upload to S3
                        s3_client.upload_file(file_name, bucket, file_name, ExtraArgs={'ContentType': 'image/png'})
                        os.remove(file_name)

                        print "Tile %d finished" % count
                        count += 1
        else:
            # Will only get here if there were venues in pts that are now gone due to clustering
            print 'Skipping due to lack of clusters'


# Generate a heatmap tile for the requested category, zoom level, and (x, y) coordinates
# (x, y) are the point coordinates of the tile's NW corner at the requested zoom level.
def generate_tile(category, zoom, x, y):

    print "Running category: {0} at zoom: {1} for tile ({2}, {3})".format(category, zoom, x, y)

    # Get lat, lng of SW corner (y + 1)
    min_coords = convert_point_to_coords(x, y + 1, zoom)

    # Get lat, lng of NE corner (x + 1)
    max_coords = convert_point_to_coords(x + 1, y, zoom)

    # Get all venues within the tile
    pts = []
    with conn.cursor() as cur:
        query = "SELECT lat, lng FROM heatmaps_venues WHERE category_id={0} AND geom_globe && ST_MakeEnvelope({1}, {2}, {3}, {4}, 4326) LIMIT {5};".format(category, min_coords[1], min_coords[0], max_coords[1], max_coords[0], 30000)
        cur.execute(query)
        for row in cur:
            # REMEMBER: x = lng, y = lat
            # Stored as (lng, lat)
            pts.append((float(row[1]), float(row[0])))

    print "Processing %d points..." % len(pts)

    # If there's points...though there should always be
    if len(pts) > 0:
        # If we want to cluster, cluster
        if ZOOM_PARAMETERS[zoom]['cluster']:
            kms_per_radian = 6371.0088
            epsilon = ZOOM_PARAMETERS[zoom]['cluster_epsilon'] / kms_per_radian
            db = DBSCAN(eps=epsilon, min_samples=ZOOM_PARAMETERS[zoom]['cluster_neighbors'], algorithm='ball_tree', metric='haversine').fit(np.radians(np.matrix(pts)))
            for idx, val in reversed(list(enumerate(db.labels_))):
                # Remove all the non-clustered points that reside in the [-1] index
                if val == -1:
                    # Stored as (lng, lat)
                    pts.remove((pts[idx][0], pts[idx][1]))

        # Normalize the pts now
        tile_pts = []
        for pt in pts:
            lat = ((max_coords[0] - pt[1]) / (max_coords[0] - min_coords[0])) * (TILE_RESOLUTION - 1)
            lng = ((pt[0] - min_coords[1]) / (max_coords[1] - min_coords[1])) * (TILE_RESOLUTION - 1)
            tile_pts.append((lng, lat))

        # tile_pts now has normalized (x, y) values for every venue
        # If we're clustering, only proceed if we have pts
        if len(tile_pts) > 0:
            # Generate a heatmap!
            hm = heatmap.Heatmap()
            img = hm.heatmap(tile_pts, opacity=ZOOM_PARAMETERS[zoom]['opacity'], dotsize=ZOOM_PARAMETERS[zoom]['dotsize'], size=(TILE_RESOLUTION, TILE_RESOLUTION), color_scheme='th_classic')

            file_name = "{0}_{1}_{2}_{3}.png".format(category, zoom, x, y)
            img.save(file_name, format="PNG")

            # Upload to S3
            s3_client.upload_file(file_name, bucket, file_name, ExtraArgs={'ContentType': 'image/png'})
            os.remove(file_name)
            print 'Uploaded to S3'

        else:
            # Will only get here if there were venues in pts that are now gone due to clustering
            print 'Skipping due to lack of clusters'


def compress_tiles():
    print "Compressing tiles..."
    for file_name in sorted(os.listdir(os.getcwd())):
        if not (file_name.startswith('.') or file_name.startswith('FULL')):
            os.system("pngcrush -m 7 -q -ow {0}".format(file_name))


def threaded_upload_to_s3():
    for file_name in sorted(os.listdir(os.getcwd())):
        if not (file_name.startswith('.') or file_name.startswith('FULL')):
            threading.Thread(target=upload_to_s3, args=(file_name,)).start()


def upload_to_s3(file_name):
    print "LOOKING TO UPLOAD {0}".format(file_name)
    try:
        s3_client.head_object(Bucket=bucket, Key=file_name)
    except:
        print "Uploading {0}...".format(file_name)
        s3_client.upload_file(file_name, bucket, file_name, ExtraArgs={'ContentType': 'image/png'})
    os.remove(file_name)


if __name__ == "__main__":
    # Establish RDS connection
    try:
        conn = psycopg2.connect(host=rds_host, port=5432, user=user_name, password=password, dbname=db_name)
    except psycopg2.Error:
        print "ERROR: Unexpected error: Could not connect to Postgres instance."
        sys.exit()

    print "SUCCESS: Connection to RDS Postgres instance succeeded"

    # Set working directory to '/heatmaps' to save the tiles
    os.chdir(os.getcwd() + '/heatmaps')

    # Coords to run
    coord = [40.74, -74]
    print "RUNNING {0} {1}".format(coord[0], coord[1])
    generate_all_heatmaps_by_city(coord[0], coord[1], RUN_BY_ZOOM)
    compress_tiles()
    threaded_upload_to_s3()
