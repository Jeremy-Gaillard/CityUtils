import sys
import argparse
import yaml
import psycopg2
from texture_atlas import TextureAtlas
from wand.image import Image
from wand.color import Color

if __name__ == '__main__':

    """
    # arg parse
    descr = 'Process a database to build an octree for building-server'
    parser = argparse.ArgumentParser(description=descr)

    cfg_help = 'configuration file'
    parser.add_argument('cfg', metavar='cfg', type=str, help=cfg_help)

    city_help = 'city to process'
    parser.add_argument('city', metavar='city', type=str, help=city_help)

    score_help = 'score function with "ST_Area(Box2D(geom))" as default value'
    parser.add_argument('--score', metavar='score', type=str, help=score_help,
                        default="ST_Area(Box2D(geom))")

    args = parser.parse_args()

    # load configuration
    ymlconf_cities = None
    with open(args.cfg, 'r') as f:
        try:
            ymlconf_cities = yaml.load(f)['cities']
        except:
            print("ERROR: ", sys.exc_info()[0])
            f.close()
            sys.exit()

    ymlconf_db = None
    with open(args.cfg, 'r') as f:
        try:
            ymlconf_db = yaml.load(f)['flask']
        except:
            print("ERROR: ", sys.exc_info()[0])
            f.close()
            sys.exit()

    # check if the city is within the configuration
    if args.city not in ymlconf_cities:
        print(("ERROR: '{0}' city not defined in configuration file '{1}'"
               .format(args.city, args.cfg)))
        sys.exit()

    # get city configuration
    cityconf = ymlconf_cities[args.city]

    # check if the configuration is well defined for the city
    if (("tablename" not in cityconf) or ("extent" not in cityconf)
            or ("maxtilesize" not in cityconf)):
        print(("ERROR: '{0}' city is not properly defined in '{1}'"
              .format(args.city, args.cfg)))
        sys.exit()


    # open database
    app = type('', (), {})()
    app.config = ymlconf_db
    Session.init_app(app)
    utils.CitiesConfig.init(str(args.cfg))
    """

    # Temp hardwritten configuration
    db = psycopg2.connect(
        "postgresql://jeremy:jeremy@localhost:5432/temp"
    )
    db.autocommit = True
    cursor = db.cursor()

    # test
    #test = utils.CitiesConfig.allRepresentations(args.city, "buildings")
    t_destination = "lod2"
    t_temp = "temp_table"
    epsg = 3946
    imageRootDir = "/media/data/Backup villes/DataLyon_Archives/LYON_1ER_2012"
    imageOutputDir = "/media/data/Backup villes/DataLyon_Archives/LYON_1ER_2012"

    # Drop tables
    query = "DROP TABLE IF EXISTS {0}".format(t_destination)
    cursor.execute(query)
    query = "DROP TABLE IF EXISTS {0}".format(t_temp)
    cursor.execute(query)

    # Create destination table
    query = "CREATE TABLE {0} (gid SERIAL PRIMARY KEY, geom geometry(MultiPolygonZ, {1}), uv geometry(MultiPolygon), texture_uri varchar(4000))".format(t_destination, epsg)
    cursor.execute(query)

    # Import data from 3DCityDB
    query = ("CREATE TABLE {0} AS SELECT building.id AS gid, "
        "ST_Collect(surface_geometry.geometry) AS geom, "
        "ST_Collect(ST_Translate(ST_Scale(textureparam.texture_coordinates, 1, -1), 0, 1)) AS uv, "
        "tex_image_uri AS uri FROM building JOIN "
        "thematic_surface ON building.id=thematic_surface.building_id JOIN "
        "surface_geometry ON surface_geometry.root_id="
        "thematic_surface.lod2_multi_surface_id JOIN textureparam ON "
        "textureparam.surface_geometry_id=surface_geometry.id "
        "JOIN surface_data ON textureparam.surface_data_id=surface_data.id "
        "JOIN tex_image ON surface_data.tex_image_id=tex_image.id "
        "GROUP BY building.id, tex_image_uri").format(t_temp)
    cursor.execute(query)

    query = "SELECT gid, uri FROM {0}".format(t_temp)
    cursor.execute(query)

    buildings = {}
    for gid, uri in cursor.fetchall():
        if gid not in buildings:
            buildings[gid] = []
        buildings[gid].append(uri)

    for gid in buildings:
        images = []
        for uri in buildings[gid]:
            images.append(Image(filename=imageRootDir + "/" + uri))
            print(uri)
        atlas = TextureAtlas.from_texture_array(images)
        newUri = '{0}/atlas-{1}.jpg'.format(imageOutputDir, gid)
        atlas.getTexture().save(filename=newUri)
        for i, img in enumerate(buildings[gid]):
            transform = atlas.getTransform(i)
            query = "UPDATE {0} SET uv = ST_TRANSLATE(ST_SCALE(uv, {1}, {2}), {3}, {4}) WHERE uri='{5}' AND gid={6}".format(t_temp, transform[1][0], transform[1][1], transform[0][0], transform[0][1], img, gid)
            print(transform)
            cursor.execute(query)
        query = "WITH t AS (SELECT gid, (ST_Dump(geom)).geom AS geom, (ST_Dump(geom)).path AS geomid, uri FROM {2} WHERE gid={3}), d AS (SELECT gid, (ST_Dump(uv)).geom AS uv, (ST_Dump(geom)).path AS uvid, uri FROM {2} WHERE gid={3}) INSERT INTO {0} SELECT t.gid, ST_Collect(geom), ST_Collect(uv), '{1}' FROM t JOIN d ON t.uri=d.uri AND geomid=uvid GROUP BY t.gid".format(t_destination, newUri, t_temp, gid)
        cursor.execute(query)

    query = "DROP TABLE {0}".format(t_temp)
    cursor.execute(query)
