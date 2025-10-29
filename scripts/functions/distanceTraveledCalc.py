'''
SCRIPT TO CALCULATE THE DISTANCES BETWEEN ARENAS

haversine does distance between 2 pts on sphere aka flying
    even though i am using arenas and not airports. its good enough for my purposes

openRouteService requires a free api key to calculate the driving distances between
the 2 arenas

THIS ONLY NEEDS TO BE RUN ONCE UNLESS A NEW ARENA IS BUILT OR ADDED

LAST RUN WAS 2024 WHEN THE INTUIT DOME OPENED

'''
filename_arena_geos = 'arenas.csv'
filename_teamName_flight_distances = 'arenaDistanceFlightMatrix.csv'
filename_teamName_drive_distances = 'arenaDistanceDriveMatrix.csv'


from haversine import haversine, Unit
import openrouteservice
import pandas as pd
import itertools

#### importing custom modules from the projects folder
import sys
from pathlib import Path
# Start at current working directory
current = Path.cwd()
# Walk up the tree until config.py is found or root is reached
for parent in [current] + list(current.parents):
    config_path = parent / "config.py"
    if config_path.exists():
        sys.path.append(str(parent))
        import config # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< 
        break
else:
    raise FileNotFoundError("config.py not found in any parent directories")
# ----------------------------------------------------------


def calc_flight_distances(
        input_geo_file = config.ARENA_DISTANCES_DIR / filename_arena_geos,
        output = config.ARENA_DISTANCES_DIR / filename_teamName_flight_distances
    ):
    # import file that has the arena lat long coords 
    locData = pd.read_csv(input_geo_file)
    locData = locData[['team', 'lat', 'long']]

    # Build all coordinate pairs
    pairs = list(itertools.product(locData.itertuples(index=False), repeat=2))

    # Compute pairwise distances (Unit.MILES for miles, can change to other units if needed)
    matrix = pd.DataFrame(
        [
            {
                'from': p1.team,
                'to': p2.team,
                'dist_mi': haversine((p1.lat, p1.long), (p2.lat, p2.long), unit=Unit.MILES)
            }
            for p1, p2 in pairs
        ]
    )
    # convert to matrix structure 
    distance_matrix = matrix.pivot(index='from', columns='to', values='dist_mi')
    distance_matrix.to_csv(
        output,
        index=False
    )
    print('flight distances saved here:', output)
    return

def calc_driving_distances(
    input_geo_file = config.ARENA_DISTANCES_DIR / filename_arena_geos,
    output = config.ARENA_DISTANCES_DIR / filename_teamName_drive_distances  
):
    '''
    FOR SOME REASON THIS DOESN'T CALC THE DISTANCES FOR PHILLY. SOMETHING To DO 
    WITH THE FREE API AND DISTANCE LIMITS. MANUALY FILLED IN THE DATA
    '''
    # import file that has the arena lat long coords 
    locData = pd.read_csv(input_geo_file)
    df_coords = locData[['team', 'long', 'lat']]

    # Convert DataFrame to (lon, lat) tuples
    coords = df_coords[['long', 'lat']].to_records(index=False).tolist()

    client = openrouteservice.Client(key=config.KEY_OPENROUTESERVICE)

    # Request driving distance matrix
    matrix = client.distance_matrix(
        locations=coords,
        metrics=['distance'], # RETURNS DISTANCE IN METERS
        profile='driving-car'
    )
    # Extract and convert from meters to miles
    distances_miles = [
        [(d / 1609.34) if d is not None else None for d in row]
        for row in matrix['distances']
    ]

    # Create distance matrix DataFrame
    df_matrix = pd.DataFrame(distances_miles, index=df_coords['team'], columns=df_coords['team'])

    df_matrix.to_csv(
        output,
        index=False
    )
    print('driving distances saved here:', output)
    return


if __name__ == "__main__":
    calc_flight_distances(
        input_geo_file = config.ARENA_DISTANCES_DIR / filename_arena_geos,
        output = config.ARENA_DISTANCES_DIR / filename_teamName_flight_distances
    )

    calc_driving_distances(
        input_geo_file = config.ARENA_DISTANCES_DIR / filename_arena_geos,
        output = config.ARENA_DISTANCES_DIR / filename_teamName_drive_distances  
    )   

