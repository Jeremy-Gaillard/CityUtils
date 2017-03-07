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
    t_destination = "test.lod1_buildings"
    epsg = 3946

    # Drop tables
    query = "DROP TABLE IF EXISTS {0}".format(t_destination)
    cursor.execute(query)

    # Create destination table
    query = "CREATE TABLE {0} (gid integer PRIMARY KEY, geom geometry(MultiPolygon, {1}), zmin real, zmax real)".format(t_destination, epsg)
    cursor.execute(query)

    # Generalise buildings
    query = "INSERT INTO {0} SELECT gid, ST_Multi(ST_ConvexHull(ST_Force2D(ST_ForceCollection(geom)))), ST_ZMin(geom), ST_zmax(geom) FROM {1}".format(t_destination, t_source)
    cursor.execute(query)
