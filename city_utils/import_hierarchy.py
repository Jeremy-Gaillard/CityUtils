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
        "postgresql://hme:jeremy@localhost:5432/lyon"
    )
    db.autocommit = True
    cursor = db.cursor()

    # Create schema?

    t_hierarchy = "test.hierarchy"
    t_tiles = "test.tiles"
    t_buildings = "test.buildings"
    t_buildings_data = "split_mp_roofs"
    epsg = 3946
    db_name = "lyon"
    """shp = ["test_files/Lyon/Ilots_reseauroutier_v3.shp test.tiles",
        "test_files/Lyon/Ilots_reseauroutier_v2.shp test.tiles",
        "test_files/Lyon/Ilots_reseauroutier.shp test.tiles"]"""

    shp = ["/home/hme/data/shp/resultat-adr_voie_lieu.adrarrond/adr_voie_lieu.adrarrond.shp",
           "/home/hme/data/shp/resultat-vdl_vie_citoyenne.perimetre_de_quartier/vdl_vie_citoyenne.perimetre_de_quartier.shp",
           "/home/hme/data/shp/resultat-vdl_vie_citoyenne.contour_de_bureau_de_vote/vdl_vie_citoyenne.contour_de_bureau_de_vote.shp",
           "/home/hme/data/shp/routes/routes.shp"]

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
    maxDepth = len(shp) - 1

    # Rename columns
    query = "ALTER TABLE {0} RENAME COLUMN geom TO footprint".format(t_tiles)
    cursor.execute(query)
    query = "ALTER TABLE {0} RENAME gid TO tile".format(t_tiles)
    cursor.execute(query)
    query = "CREATE INDEX tiles_footprint_idx ON {0} using gist(footprint)".format(t_tiles)
    cursor.execute(query)

    # Create building table
    query = "CREATE TABLE {0} (gid serial, tile integer, footprint geometry(Multipolygon, {1}))".format(t_buildings, epsg)
    cursor.execute(query)
    query = "CREATE INDEX buildings_gid_idx ON {0} (gid)".format(t_buildings)
    cursor.execute(query)
    query = "CREATE INDEX buildings_tile_idx ON {0} (tile)".format(t_buildings)
    cursor.execute(query)

    # Populate building table and compute footprints
    query = "INSERT INTO {0} (gid, footprint) SELECT gid, ST_Multi(ST_Buffer(St_Force2D(geom), 0)) FROM {1}".format(t_buildings, t_buildings_data)
    cursor.execute(query)
    query = ("UPDATE {0} t SET footprint = a.geom FROM ("
                " SELECT gid, ST_Collect(ST_MakePolygon(geom)) AS geom"
                " FROM ("
                    "SELECT gid, ST_NRings(footprint) AS nrings,"
                        "ST_ExteriorRing((ST_Dump(footprint)).geom) AS geom"
                    " FROM {0}"
                    " WHERE ST_NRings(footprint) > 1"
                    ") s"
                " GROUP BY gid, nrings"
                " HAVING nrings > COUNT(gid)"
                ") a"
            " WHERE t.gid = a.gid;").format(t_buildings)
    cursor.execute(query)

    # Check for building-tile interserctions
    query = ("WITH t AS (SELECT tile, footprint FROM {0} WHERE depth={1}),"
             " d AS (SELECT t.tile, {2}.gid FROM t, {2} WHERE st_intersects("
             "t.footprint, {2}.footprint)), e AS (SELECT gid, count(*) AS cnt"
             " FROM d GROUP BY gid) SELECT d.gid, d.tile FROM d JOIN e ON "
             " d.gid=e.gid WHERE cnt>1;").format(t_tiles, maxDepth, t_buildings)
    cursor.execute(query)
    gid2Tiles = {}
    tile2Group = {}
    tileGroups = []
    for (gid, tile) in cursor:
        if gid not in gid2Tiles:
            gid2Tiles[gid] = []
        gid2Tiles[gid].append(tile)
    for gid in gid2Tiles:
        group = { tile for tile in gid2Tiles[gid] }
        for tile in gid2Tiles[gid]:
            if tile in tile2Group:
                if tile2Group[tile] in tileGroups:
                    tileGroups.remove(tile2Group[tile])
                group.update(tile2Group[tile])
            tile2Group[tile] = group
        tileGroups.append(group)

    # Merge tiles with overlapping objects
    for group in tileGroups:
        condition = " OR ".join(["tile={0}".format(tile) for tile in group])
        query = "INSERT INTO {0} (footprint, depth) SELECT ST_Multi(ST_Union(footprint)), {2} FROM {0} WHERE {1}".format(t_tiles, condition, maxDepth)
        cursor.execute(query)
        query = "DELETE FROM {0} WHERE {1}".format(t_tiles, condition)
        cursor.execute(query)

    # Create hierarchy
    query = "CREATE TABLE {0} (tile serial, child integer)".format(t_hierarchy)
    cursor.execute(query)
    query = "CREATE INDEX hierarchy_tile_idx ON {0} (tile)".format(t_hierarchy)
    cursor.execute(query)

    # Link children with their parent
    for i in range(maxDepth):
        query = ("WITH t AS (SELECT * FROM {0} WHERE depth={1}),"
                "d AS (SELECT * FROM {0} WHERE depth={1}+1)"
                "SELECT t.tile, d.tile FROM t, d WHERE ST_Intersects("
                "ST_Centroid(d.footprint), t.footprint)").format(t_tiles, i)
        cursor.execute(query)
        results = cursor.fetchall()
        for parent, child in results:
            query = "INSERT INTO {0} (tile, child) VALUES ({1}, {2})".format(
                    t_hierarchy, parent, child)
            cursor.execute(query)

    # Snap parent tile boundaries to the ones of their children
    # TODO: keep make valid ?
    for i in reversed(range(maxDepth)):
        query = ("WITH t AS (SELECT {1}.tile, ST_Union(ST_MakeValid(footprint))"
                " AS reference FROM {0} JOIN {1} ON {0}.tile={1}.child WHERE"
                " depth={2} GROUP BY {1}.tile) UPDATE {0} SET footprint="
                "ST_Multi(ST_Buffer(ST_Buffer(reference, 2),-2)) FROM t"
                " WHERE {0}.tile=t.tile").format(t_tiles, t_hierarchy, i + 1)
        cursor.execute(query)

    # Building indexation
    print("Indexing")
    query = ("WITH d AS (SELECT tile, footprint FROM {0} WHERE depth={2})"
            "UPDATE {1} SET tile=d.tile FROM d WHERE ST_Intersects(d.footprint, {1}.footprint)").format(t_tiles, t_buildings, maxDepth)
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
