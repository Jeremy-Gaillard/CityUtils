import psycopg2
import subprocess

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

    # Create schema?

    t_hierarchy = "test.hierarchy"
    t_tiles = "test.tiles"
    t_buildings = "test.buildings"
    t_buildings_data = "test.lod2"
    epsg = 3946
    db_name = "lyon"
    shp = ["test_files/Lyon/Ilots_reseauroutier_v3.shp test.tiles",
        "test_files/Lyon/Ilots_reseauroutier_v2.shp test.tiles",
        "test_files/Lyon/Ilots_reseauroutier.shp test.tiles"]

    # Drop tables
    query = "DROP TABLE IF EXISTS {0}".format(t_buildings)
    cursor.execute(query)
    query = "DROP TABLE IF EXISTS {0}".format(t_hierarchy)
    cursor.execute(query)
    query = "DROP TABLE IF EXISTS {0}".format(t_tiles)
    cursor.execute(query)

    # Create tile table
    commandLine = "shp2pgsql -p -s {0} {1} {2}".format(epsg, shp[-2], t_tiles)
    pgsql = subprocess.Popen(commandLine.split(), stdout=subprocess.PIPE)
    subprocess.call(("psql", db_name), stdin=pgsql.stdout)

    query = "ALTER TABLE {0} ADD COLUMN depth smallint".format(t_tiles)
    cursor.execute(query)
    query = "ALTER TABLE {0} ADD COLUMN bbox Box3D".format(t_tiles)
    cursor.execute(query)
    #query = "CREATE INDEX tiles_tile_idx ON {0} (gid)".format(t_tiles)
    #cursor.execute(query)

    # Import tile data
    for i, f in enumerate(shp):
        commandLine = "shp2pgsql -a -s {0} {1} {2}".format(epsg, f, t_tiles)
        pgsql = subprocess.Popen(commandLine.split(), stdout=subprocess.PIPE)
        subprocess.call(("psql", db_name), stdin=pgsql.stdout)
        query = "UPDATE {0} SET depth={1} WHERE depth is null".format(t_tiles, str(i))
        cursor.execute(query)

    # Rename columns
    query = "ALTER TABLE {0} RENAME COLUMN geom TO footprint".format(t_tiles)
    cursor.execute(query)
    query = "ALTER TABLE {0} RENAME gid TO tile".format(t_tiles)
    cursor.execute(query)
    query = "CREATE INDEX tiles_footprint_idx ON {0} using gist(footprint)".format(t_tiles)
    cursor.execute(query)

    # Create hierarchy
    query = "CREATE TABLE {0} (tile serial, child integer)".format(t_hierarchy)
    cursor.execute(query)
    query = "CREATE INDEX hierarchy_tile_idx ON {0} (tile)".format(t_hierarchy)
    cursor.execute(query)

    query = "SELECT tile, fid, id_parent, depth from {0}".format(t_tiles)
    cursor.execute(query)

    parentOf = {}
    fidToGid = {}
    for gid, fid, parentId, depth in cursor:
        fidToGid[(int(fid), depth)] = gid
        if parentId != None:
            parentOf[gid] = (int(parentId), depth - 1)

    tileChild = [(fidToGid[parentOf[gid]], gid) for gid in parentOf]
    query = "INSERT INTO {0}".format(t_hierarchy)
    # Suboptimal, see http://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query
    cursor.executemany(query + " VALUES (%s, %s)", tileChild)

    # Create building table
    query = "CREATE TABLE {0} (gid serial, tile integer, footprint geometry(Multipolygon, {1}))".format(t_buildings, epsg)
    cursor.execute(query)
    query = "CREATE INDEX buildings_gid_idx ON {0} (gid)".format(t_buildings)
    cursor.execute(query)
    query = "CREATE INDEX buildings_tile_idx ON {0} (tile)".format(t_buildings)
    cursor.execute(query)

    # Building indexation
    print("Indexing")
    maxDepth = len(shp) - 1
    query = ("WITH d AS (SELECT tile, footprint FROM {0} WHERE depth={4})"
            "INSERT INTO {1} (gid, tile) SELECT gid, d.tile FROM {2} INNER JOIN d ON ST_Intersects(d.footprint, ST_Centroid(ST_SetSRID(Box2D({2}.geom), {3})));".format(t_tiles, t_buildings, t_buildings_data, epsg, maxDepth))
    cursor.execute(query)

    # Compute bbox
    print("Computing bbox")
    query = ("WITH t AS (SELECT {2}.tile, ST_ZMin(ST_3DExtent(geom)) AS zmin, ST_ZMax(ST_3DExtent(geom)) AS zmax FROM {1} JOIN {2} ON {1}.gid={2}.gid WHERE tile IS NOT NULL GROUP BY {2}.tile)"
             "UPDATE {0} SET bbox=Box3D(ST_Translate(ST_Extrude(footprint, 0, 0, zmax-zmin), 0, 0, zmin)) FROM t WHERE t.tile = {0}.tile".format(t_tiles, t_buildings_data, t_buildings))
    cursor.execute(query)
    for i in reversed(range(maxDepth)):
        query = ("WITH t AS (SELECT {0}.tile, {0}.child FROM {0} INNER JOIN {1} ON {0}.tile={1}.tile WHERE depth={2}), "
                 "d AS (SELECT t.tile, Box3D(ST_3DExtent(bbox)) AS box FROM t INNER JOIN {1} ON t.child={1}.tile GROUP BY t.tile) "
                 "UPDATE {1} SET bbox=box FROM d WHERE d.tile={1}.tile".format(t_hierarchy, t_tiles, i))
        cursor.execute(query)
