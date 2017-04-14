import psycopg2
import time

if __name__ == '__main__':
    # Temp hardwritten configuration
    db = psycopg2.connect(
        "postgresql://jeremy:jeremy@localhost:5432/lyon"
    )
    db.autocommit = True
    cursor = db.cursor()

    # test
    #test = utils.CitiesConfig.allRepresentations(args.city, "buildings")
    t_source = "test.lod2"
    t_features = "test.buildings"
    t_tiles = "test.tiles"
    maxDepth = 3

    # Trivial case: the building is alone in the tile, so its footprint is the footprint of the tile
    query = "WITH a AS (SELECT tile, count(*) AS num FROM {0} GROUP BY tile) UPDATE {0} SET footprint={1}.footprint FROM a INNER JOIN {1} ON a.tile={1}.tile WHERE num=1 AND a.tile={0}.tile".format(t_features, t_tiles)
    cursor.execute(query)


    cursor.execute("DROP TABLE IF EXISTS test.test")
    cursor.execute("CREATE TABLE test.test (tile integer, poly geometry, feature_gid integer)")

    # Get only tile with more than one building
    query = "WITH a AS (SELECT tile, count(*) AS num FROM {0} GROUP BY tile) SELECT tile FROM a WHERE num!=1".format(t_features)
    cursor.execute(query)

    tuples = cursor.fetchall();
    t0 = time.time()
    for i, t in enumerate(tuples):
        tileId = t[0]
        if i < 1254:
            continue

        print("Processing tile " + str(tileId))
        # Break down polyhedral surfaces into polygons
        a = "SELECT {0}.gid, ST_GeometryN(geom, generate_series(1, ST_NumGeometries(geom))) AS poly FROM {0} INNER JOIN {1} ON {0}.gid={1}.gid WHERE tile={2}".format(t_source, t_features, tileId)
        # Compute the union of the polygons projected in the 2D space
        b = "SELECT gid, ST_Union(ST_MakeValid(ST_Force2D(poly))) AS poly FROM a GROUP BY gid"
        # Compute the difference between the features' polygons and the bufferised tile
        # TODO: replace 100 by max(x,y) ?
        c = "SELECT tile, ST_Difference(ST_Buffer(footprint, 200, 1), ST_Collect(poly)) AS holed FROM b, {0} WHERE tile={1} GROUP BY tile".format(t_tiles, tileId)
        # Compute skeleton
        d = "SELECT tile, ST_StraightSkeleton(holed) AS skeleton FROM c"
        # Add the holed buffered tile boundary to create a closed graph
        e = "SELECT d.tile, ST_Union(skeleton, ST_Boundary(holed)) AS graph FROM d, c"
        # Polygonise the graph
        f = "SELECT tile, ST_Polygonize(graph) AS polygons FROM e GROUP BY tile"
        # Break down the geometry collection into polygons, compute the intersection between them and the original tile
        g = "INSERT INTO test.test SELECT f.tile, ST_Intersection(ST_GeometryN(polygons, generate_series(1, ST_NumGeometries(polygons))), footprint) AS poly FROM f, {0} WHERE {0}.tile = {1}".format(t_tiles, tileId)
        #query = "CREATE TABLE test.test AS ({0})".format("WITH a AS ({0}), b AS ({1}), c AS ({2}), d AS({3}), e AS({4}), f AS({5}) {6}".format(a, b, c, d, e, f, g))
        query = "WITH a AS ({0}), b AS ({1}), c AS ({2}), d AS({3}), e AS({4}), f AS({5}) {6}".format(a, b, c, d, e, f, g)
        try:
            cursor.execute(query)
        except psycopg2.InternalError as err:
            print('{0} generated an error: "{1}"'.format(tileId, err))
            # Skip tile
            continue


        h = "UPDATE test.test SET feature_gid=gid FROM a WHERE tile={0} AND ST_Intersects(a.poly, test.test.poly)".format(tileId)
        query = "WITH a AS ({0}) {1}".format(a, h)
        cursor.execute(query)

        #query = "CREATE TABLE test.test2 AS (SELECT feature_gid, ST_Union(poly) FROM test.test GROUP BY feature_gid)"
        query = "WITH a AS (SELECT feature_gid, ST_Multi(ST_Union(poly)) as fp FROM test.test GROUP BY feature_gid) UPDATE {0} SET footprint=fp FROM a WHERE feature_gid=gid".format(t_features)
        cursor.execute(query)

        cursor.execute("DELETE FROM test.test")

        print(str(i + 1) + "/" + str(len(tuples)) + " ; time elapsed = " + str(time.time() - t0))
