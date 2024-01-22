import argparse
import functools
import operator

import numpy as np
import polars as pl

# should be using this https://biobank.ndph.ox.ac.uk/showcase/field.cgi?id=20400
# plus the intervening time from first assessment plus age at first assessment
# for age instead of just
# https://biobank.ndph.ox.ac.uk/showcase/field.cgi?id=21022

# this is the paper for the data we are working with
# https://www.cambridge.org/core/journals/bjpsych-open/article/mental-health-in-uk-biobank-development-implementation-and-results-from-an-online-questionnaire-completed-by-157-366-participants-a-reanalysis/F402F460E7731030354A07F9AD8F46A1#article
# we should make sure to mention the cohort and response biases and their implications

# there's also the data set described here: https://biobank.ndph.ox.ac.uk/showcase/ukb/docs/mwb_overview.pdf
# should we include the audit from that? For participants who've answered twice, we can default to the first
# we can include a covariate to control for differences

# should we split audit qs into many binary phenotypes (e.g. <=1 or >=2)?

parser = argparse.ArgumentParser()
parser.add_argument('outdir')
parser.add_argument('year_of_birth_csv')
parser.add_argument('month_of_birth_csv')
parser.add_argument('audit_completion_date_csv')
parser.add_argument('audit_question_csvs', nargs=10)

args = parser.parse_args()

birth_year_df = pl.read_csv(
    args.year_of_birth_csv,
    separator='\t'
)
birth_month_df = pl.read_csv(
    args.month_of_birth_csv,
    separator='\t'
)
completion_date_df = pl.read_csv(
    args.audit_completion_date_csv,
    separator='\t'
)

age_df = birth_year_df.join(
    birth_month_df,
    how = 'inner',
    on = 'eid'
).join(
    completion_date_df,
    how = 'inner',
    on = 'eid'
).with_columns((
    pl.col('20400-0.0').str.to_date('%Y-%m-%d').cast(int) -
    (
        pl.col('34-0.0').cast(str) + ' ' + pl.col('52-0.0').cast(str).str.pad_start(2, '0')
    ).str.to_date('%Y %m').cast(int)
).alias('rough_age_in_days'))

audit_dfs = [
    pl.read_csv(
        csv_fname,
        separator='\t'
    ) for csv_fname in args.audit_question_csvs
]

df = audit_dfs[0]
for other_df in audit_dfs[1:]:
    df = df.join(
        other_df,
        how = 'outer',
        on = 'eid'
    )

fields = [
    '20414-0.0',
    '20403-0.0',
    '20416-0.0',
    '20413-0.0',
    '20407-0.0',
    '20412-0.0',
    '20409-0.0',
    '20408-0.0',
    '20411-0.0',
    '20405-0.0'
]

# set prefer not to answer to nones
for field in fields:
    df = df.with_columns([pl.when(pl.col(field) == -818).then(None).otherwise(pl.col(field)).alias(field)])

# rescale
for field in fields[1:8]:
    df = df.with_columns([pl.col(field) - 1])

for field in fields[8:]:
    df = df.with_columns([pl.col(field)*2])

# gating logic
for field in fields[1:8]:
    df = df.with_columns([pl.when(pl.col(fields[0]) == 0).then(pl.lit(0)).otherwise(pl.col(field)).alias(field)])

for field in fields[3:8]:
    df = df.with_columns([pl.when(pl.col(fields[1]) + pl.col(fields[2]) == 0).then(pl.lit(0)).otherwise(pl.col(field)).alias(field)])

# did not remove individuals with prefer not to answers. Simply keeping them in, just with nulls
# nulls should propogate through addition

# calculate audit composite scores
audit_sum_fields = ['audit_c', 'audit_p', 'audit_t']
audit_log_fields = [f'{field}_log10' for field in audit_sum_fields]
df = df.with_columns([
    functools.reduce(operator.add, [pl.col(field) for field in fields[:3]]).alias('audit_c'),
    functools.reduce(operator.add, [pl.col(field) for field in fields[3:]]).alias('audit_p'),
    functools.reduce(operator.add, [pl.col(field) for field in fields]).alias('audit_t'),
]).with_columns([
    (pl.col(sum_field) + 1).log10().alias(log_field) for sum_field, log_field in zip(audit_sum_fields, audit_log_fields)
])

df = df.filter(
    pl.any_horizontal(~pl.all().exclude('eid').is_null())
)

print('N at least one response', df.shape[0])

# add age
df = df.join(
    age_df,
    how='inner',
    on='eid'
).filter(
    ~pl.col('rough_age_in_days').is_null()
)

print('n left with age', df.shape[0])

df.rename(
    {'eid': 'ID'}
).write_csv(
    f'{args.outdir}/full_audit.tab',
    separator='\t'
)

for idx, field in enumerate(fields):
    np.save(
        f'{args.outdir}/audit_q{idx+1}',
        df.select(
            'eid',
            field,
            'rough_age_in_days'
        ).filter(
            ~pl.col(field).is_null()
        ).to_numpy()
    )

for field in audit_sum_fields + audit_log_fields:
    np.save(
        f'{args.outdir}/{field}',
        df.select(
            'eid',
            field,
            'rough_age_in_days'
        ).filter(
            ~pl.col(field).is_null()
        ).to_numpy()
    )
