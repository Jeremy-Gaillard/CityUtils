import sys
import argparse
import yaml
import psycopg2

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
        "postgresql://jeremy:jeremy@localhost:5432/lyon"
    )
    db.autocommit = True
    cursor = db.cursor()

    # test
    #test = utils.CitiesConfig.allRepresentations(args.city, "buildings")
    t_source = "test.lod2"
    t_tiles = "test.tiles"
    t_hierarchy = "test.hierarchy"
    t_destination = "test.lod1"
    epsg = 3946

    # Drop tables
    query = "DROP TABLE IF EXISTS {0}".format(t_destination)
    cursor.execute(query)

    # Create destination table
    query = "CREATE TABLE {0} (tile integer PRIMARY KEY, geom geometry(MultiPolygon, {1}), zmin real, zmax real)".format(t_destination, epsg)
    cursor.execute(query)


    query = "SELECT max(depth) FROM {0}".format(t_tiles)
    cursor.execute(query)
    maxDepth = cursor.fetchone()[0]

    # Generalise leaf tiles from lod2 geometry
    query = ("WITH t AS (SELECT tile, ST_3Dextent(geom) AS box, ST_Multi(ST_ConcaveHull(ST_force2D(ST_Collect(('0106' || substring(geom::text from 5))::geometry)), 0.99)) AS hull FROM {0} WHERE tile IS NOT NULL GROUP BY tile)"
             "INSERT INTO {1} (tile, geom, zmin, zmax) SELECT tile, hull, ST_ZMin(box), ST_ZMax(box) FROM t".format(t_source, t_destination))
    cursor.execute(query)
    query = "UPDATE {0} SET geom=ST_Multi(ST_Intersection(geom, footprint)) FROM {1} WHERE {0}.tile={1}.gid AND depth={2}".format(t_destination, t_tiles, maxDepth)
    cursor.execute(query)

    # Generalise remaining tiles from children tiles
    for i in reversed(range(maxDepth)):
        query = "SELECT {0}.tile, {0}.child FROM {0} INNER JOIN {1} ON {0}.tile={1}.gid WHERE depth={2}".format(t_hierarchy, t_tiles, i)
        query = "WITH t AS ({0}) INSERT INTO {1} (SELECT t.tile, ST_Multi(ST_ConcaveHull(ST_Union(geom),0.99)), min(zmin), max(zmax) FROM t INNER JOIN {1} ON t.child={1}.tile GROUP BY t.tile)".format(query, t_destination)
        cursor.execute(query)
        query = "UPDATE {0} SET geom=ST_Multi(ST_Intersection(geom, footprint)) FROM {1} WHERE {0}.tile={1}.gid AND depth={2}".format(t_destination, t_tiles, i)
        cursor.execute(query)
