import pandas as pd


def normalize_by_commod(frame, column):
    frames = []

    for val in frame.commod.unique():
        # get only the df for the val you want
        newdf = frame[frame.commod == val].copy()

        # Exclude zero rows from the calculation
        zeroes = newdf[newdf[column] == 0.0]
        newdf = newdf[newdf[column] != 0.0]

        newdf['denormalized'] = newdf[column]

        # get the normalized values of mined as a list
        normalized = (newdf[column] - newdf[column].mean()) / newdf[column].std()

        # replace previous mined values with the scaled ones
        newdf[column] = normalized

        # append the new df to the list
        frames.append(newdf)
        frames.append(zeroes)

    # concatenate all the dfs
    return pd.concat(frames)


COMMODITIES_PATH = "~/Downloads/mrds/Commodity.txt"
COORDS_PATH = "~/Downloads/mrds/Coords.txt"
PRODUCTION_PATH = "~/Downloads/mrds/Production.txt"

commodities = pd.read_csv(COMMODITIES_PATH, delim_whitespace=True)
coords = pd.read_csv(COORDS_PATH, delim_whitespace=True)
production = pd.read_csv(PRODUCTION_PATH, delim_whitespace=True)


joined = coords[["dep_id", "lat_dec", "lon_dec"]].set_index('dep_id').join(commodities[["dep_id", "commod"]].set_index('dep_id')).join(production[["dep_id", "mined", "units"]].set_index('dep_id'))

mined_numeric = joined['mined'] != 'mt'
joined = joined[mined_numeric]

print("Numeralizing values...")
joined['mined'] = pd.to_numeric(joined['mined'])

print("Grouping rows by commodity...")
joined = joined.groupby([joined.index, 'commod']).agg(
    {'lat_dec': 'first',
     'lon_dec': 'first',
     'mined': sum,
     'units': 'first'})

joined = joined.reset_index()

print("Normalizing values...")

joined_normalized = normalize_by_commod(joined, 'mined')

print("Removing outliers...")
inliers = abs(joined_normalized['mined']) <= 3

joined_normalized = joined_normalized[inliers]

joined_normalized_inliers = joined_normalized.copy()
joined_normalized_inliers['mined'] = joined_normalized_inliers['denormalized']

joined_normalized_inliers = normalize_by_commod(joined_normalized_inliers, 'mined')
joined_normalized_inliers = joined_normalized_inliers.drop(['denormalized', 'units'], axis=1)

joined_normalized_inliers = joined_normalized_inliers.fillna(0)

print(joined_normalized_inliers)
print(joined_normalized_inliers.commod.unique())

joined_normalized_inliers.to_csv('./deposits.csv')





