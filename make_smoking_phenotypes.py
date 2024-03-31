import argparse

import numpy as np
import polars as pl

parser.add_argument('outdir')
parser.add_argument('year_of_birth')
parser.add_argument('month_of_birth')
parser.add_argument('date_of_death')
parser.add_argument('assessment_dates')
parser.add_argument('smoking_amt_touchscreen')
parser.add_argument('is_smoker_touchscreen')
parser.add_argument('inpatient_icd10_codes')
parser.add_argument('primary_and_inpatient_icd10_source')

args = parser.parse_args()

# make the two date dataframes
birth_dates = pl.read_csv(
    args.year_of_birth,
    separator='\t'
).join(
    pl.read_csv(
        args.month_of_birth,
        separator='\t'
    ),
    how = 'outer',
    on = 'eid'
)

birth_dates = birth_dates.rename(
    {col: col.replace('-0.0', '') for col in df.columns}
).rename(
    {'34': 'birth_year', '52': 'birth_month'}
).filter(
    ~pl.col('birth_year').is_null() & ~pl.col('birth_month').is_null()
).with_columns(
    (
        pl.col('birth_year').cast(str) + ' ' + pl.col('birth_month').cast(str).str.pad_start(2, '0')
    ).str.to_date('%Y %m').cast(int).alias('rough_birthdate_int')
).drop(['birth_year', 'birth_month'])

death_dates = pl.read_csv(
    args.date_of_death,
    separator='\t'
).with_columns(
    # not using source of death registry info as a batch effect covariate
    pl.when(
        ~pl.col('40000-0.0').is_null()
    ).then(
        pl.col('40000-0.0')
    ).otherwise(
        pl.col('40000-1.0')
    ).str.to_date('%Y-%m-%d').cast(int).alias('date_of_death_int')
).drop(
    ['40000-0.0', '40000-1.0']
)

last_update_or_death_age = birth_dates.join(
    death_dates,
    how = 'left',
    on = 'eid'
).with_columns(
    pl.when(
        ~pl.col('date_of_death_int').is_null()
    ).then(
        pl.col('date_of_death_int') - pl.col('rough_birthdate_int')
    ).otherwise(
        # for those people who are still alive, use the last reported death date
        # as a proxy for the last update date
        pl.col('date_of_death_int').max()- pl.col('rough_birthdate_int') 
    ).alias('last_update_or_death_age_in_days')
).drop(['date_of_death_int', 'rough_birthdate_int'])

# < 300 people reported being a smoker only at assessments after the first
# so we're only going to use that assessment
assessment_age = pl.read_csv(
    args.assessment_dates,
    separator='\t'
).with_columns(
    pl.col('53-0.0').str.to_date('%Y-%m-%d').cast(int).alias('assessment_date_int')
).drop([
    pl.col(f'53-{i}.0') for i in range(4)
]).join(
    birth_dates,
    how = 'inner',
    on = 'eid'
).with_columns(
    (pl.col('assessment_date_int') - pl.col('rough_birthdate_int')).alias('rough_assessment_age_in_days')
)

# load phenotypes
smoking_amt_touchscreen = pl.read_csv(
    args.smoking_amt_touchscreen,
    separator='\t'
).with_columns(
    pl.col('3456-0.0').alias('smoking_amt')
).drop(
    '3456-0.0', '3456-1.0', '3456-2.0', '3456-3.0'
).filter(
    ~pl.col('smoking_amt').is_null() 
).join(
    assessment_age,
    how='inner',
    on='eid'
)

is_smoker_touchscreen = pl.read_csv(
    args.is_smoker_touchscreen,
    separator='\t'
).with_columns(
    pl.col('1239-0.0').alias('is_smoker')
).drop(
    '1239-0.0', '1239-1.0', '1239-2.0', '1239-3.0'
).filter(
    ~pl.col('is_smoker').is_null() & pl.col('is_smoker').is_in([0, 1])
).join(
    assessment_age,
    how='inner',
    on='eid'
)

with inpatient_icd10_codes_file as open(args.inpatient_icd10_codes):
    lines = inpatient_icd10_codes_file.readlines()

ids = []
smokes = []
for line in lines[1:]:
  id_, rest = line.split('\t', 1)
  ids.append(int(id_))
  smokes.append('F17' in rest)
is_smoker_inpatient = pl.DataFrame(
    {'eid': ids, 'is_smoker': smokes}
).with_columns(
    pl.col('is_smoker').cast(int)
).join(
    last_update_or_death_age,
    how = 'inner',
    on = 'eid'
)

# is_smoker_inpatient is a subset of is_smoker_inpatient_or_primary
# so don't need to think about that

# this doesn't include any self reports
is_smoker_inpatient_or_primary = pl.read_csv(
    args.primary_and_inpatient_icd10_source,
    separator='\t'
).with_columns(
    pl.lit(True).alias('is_smoker'),
).drop('130869-0.0')

is_smoker_inpatient_or_primary = last_update_or_death_age.join(
    is_smoker_inpatient_or_primary,
    how = 'left',
    on = 'eid'
).with_columns(
    (~pl.col('is_smoker').is_null()).cast(int).alias('is_smoker')
).select([
    'eid', 'is_smoker', 'last_update_or_death_age_in_days'
])


# join self report to inpatient and primary

is_smoker_all = is_smoker_inpatient_or_primary.join(
    is_smoker_touchscreen,
    how = 'left',
    on = 'eid'
).with_columns(
    (pl.col('is_smoker') | pl.col('is_smoker_right')).cast(int).alias('is_smoker'),
).select([
    'eid', 'is_smoker', 'last_update_or_death_age_in_days'
])

is_smoker_touchscreen.write_csv(
    f'{args.outdir}/is_smoker_touchscreen.tab',
    separator='\t'
)
np.save(
    f'{args.outdir}/is_smoker_touchscreen.npy',
    is_smoker_touchscreen.to_numpy()
)

smoking_amt_touchscreen.write_csv(
    f'{args.outdir}/smoking_amt_touchscreen.tab',
    separator='\t'
)
np.save(
    f'{args.outdir}/smoking_amt_touchscreen.npy',
    smoking_amt_touchscreen.to_numpy()
)

is_smoker_inpatient.write_csv(
    f'{args.outdir}/is_smoker_inpatient.tab',
    separator='\t'
)
np.save(
    f'{args.outdir}/is_smoker_inpatient.npy',
    is_smoker_inpatient.to_numpy(),
)

is_smoker_inpatient_or_primary.write_csv(
    f'{args.outdir}/is_smoker_inpatient_or_primary.tab',
    separator='\t'
)
np.save(
    f'{args.outdir}/is_smoker_inpatient_or_primary.npy',
    is_smoker_inpatient_or_primary.to_numpy(),
)

is_smoker_all.write_csv(
    f'{args.outdir}/is_smoker_all.tab',
    separator='\t'
)
np.save(
    f'{args.outdir}/is_smoker_all.npy',
    is_smoker_all.to_numpy()
)

# can we account for source as a batch effect covariate?
# do we need to?
# would this cause us to remove death records as a source?
