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
parser.add_argument('year_of_birth_tsv')
parser.add_argument('month_of_birth_tsv')
parser.add_argument('original_audit_completion_date_tsv')
parser.add_argument('original_audit_question_tsvs', nargs=10)
parser.add_argument('new_audit_questionnaire')

args = parser.parse_args()

# make the data frame
df = pl.read_csv(
    args.year_of_birth_tsv,
    separator='\t'
)
for tsv in (args.month_of_birth_tsv, args.original_audit_completion_date_tsv, *args.original_audit_question_tsvs):
    df = df.join(
        pl.read_csv(
            tsv,
            separator='\t'
        ),
        how = 'outer',
        on = 'eid'
    )
df = df.join(
    pl.read_csv(
        args.new_audit_questionnaire,
        null_values=''
    ),
    how = 'outer',
    on = 'eid',
).drop('29189-0.0', '29101-0.0', '29102-0.0', '29103-0.0')

print('N all participants: ', df.shape[0])
df = df.filter(
    ~pl.all_horizontal(pl.col('*').exclude('eid', '34-0.0', '52-0.0').is_null())
)
print('N response participants: ', df.shape[0])

# compute ages
df = df.rename(
    {col: col.replace('-0.0', '') for col in df.columns}
).rename(
    {'34': 'birth_year', '52': 'birth_month'}
).filter(
    ~pl.col('birth_year').is_null() & ~pl.col('birth_month').is_null()
).with_columns(
    (
        pl.col('birth_year').cast(str) + ' ' + pl.col('birth_month').cast(str).str.pad_start(2, '0')
    ).str.to_date('%Y %m').cast(int).alias('rough_birthdate_int')
).with_columns(
    pl.when(
        ~pl.col('20400').is_null() & (pl.col('20400') != '')
    ).then(
        pl.col('20400').str.to_date('%Y-%m-%d').cast(int) - pl.col('rough_birthdate_int')
    ).otherwise(
        None
    ).alias('original_rough_age_in_days'),
    pl.when(
        ~pl.col('29202').is_null() & (pl.col('29202') != '')
    ).then(
        pl.col('29202').str.to_date('%Y-%m-%d').cast(int) - pl.col('rough_birthdate_int')
    ).otherwise(
        None
    ).alias('followup_rough_age_in_days')
).drop('rough_birthdate_int', '29202', '20400')

# order the fields by first question to last
# for the original questionnaire, this does not correspond to
# the order of the data field ids
original_fields = [
    '20414',
    '20403',
    '20416',
    '20413',
    '20407',
    '20412',
    '20409',
    '20408',
    '20411',
    '20405'
]
followup_fields = [str(f) for f in range(29091, 29091+10)]

df = df.rename({
    'eid': 'ID',
    **{field: f'original_q{idx+1}' for idx, field in enumerate(original_fields)},
    **{field: f'followup_q{idx+1}' for idx, field in enumerate(followup_fields)}
})

original_fields = [f'original_q{idx}' for idx in range(1, 11)]
followup_fields = [f'followup_q{idx}' for idx in range(1, 11)]

# set prefer not to answer to nones
for field in original_fields:
    df = df.with_columns([pl.when(pl.col(field) == -818).then(None).otherwise(pl.col(field)).alias(field)])
for field in followup_fields:
    df = df.with_columns([pl.when(pl.col(field) == -3).then(None).otherwise(pl.col(field)).alias(field)])

# ---- appropriately scale numeric values of responses ----
# q1 values are scaled appropriately in both questionnaires
# scale original questionnaire
for field in original_fields[1:8]:
    df = df.with_columns(pl.col(field) - 1)
for field in original_fields[8:]:
    df = df.with_columns(pl.col(field)*2)

# scale followup questionnaire
df = df.with_columns(pl.col(followup_fields[1]) - 1)
for field in followup_fields[2:8]:
    df = df.with_columns(
        pl.when(
            pl.col(field) > 0
        ).then(
            pl.col(field) - 1
        ).otherwise(
            pl.col(field)
        )
    )
for field in followup_fields[8:]:
    df = df.with_columns(pl.col(field)*2)

# gating logic
for field in original_fields[1:8]:
    df = df.with_columns(
        pl.when(
            pl.col(original_fields[0]) == 0
        ).then(
            0
        ).otherwise(
            pl.col(field)
        ).alias(field)
    )
for field in followup_fields[1:8]:
    df = df.with_columns(
        pl.when(
            pl.col(followup_fields[0]) == 0
        ).then(
            0
        ).otherwise(
            pl.col(field)
        ).alias(field)
    )

for field in original_fields[3:8]:
    df = df.with_columns(
        pl.when(
            pl.col(original_fields[1]) + pl.col(original_fields[2]) == 0
        ).then(
            0
        ).otherwise(
            pl.col(field)
        ).alias(field)
    )
for field in followup_fields[3:8]:
    df = df.with_columns(
        pl.when(
            pl.col(followup_fields[1]) + pl.col(followup_fields[2]) == 0
        ).then(
            0
        ).otherwise(
            pl.col(field)
        ).alias(field)
    )

for q_num, (original_field, followup_field) in enumerate(list(zip(original_fields, followup_fields))):
    q_num += 1
    q_df = df.filter(
        ~pl.col(original_field).is_null() &
        ~pl.col(followup_field).is_null()
    )

    ori = q_df.select(original_field).to_numpy().flatten()
    follow = q_df.select(followup_field).to_numpy().flatten()
    r2 = np.corrcoef(ori, follow)[0, 1]**2
    print(f'Q: {q_num} n shared {q_df.shape[0]} r2 {r2}')

# not removing individuals with prefer not to answers
# as this amounts to ~1.5% of respondents
# and it seems like its not worth losing that much data for a bit of cleanliness.
# Over half the respondents who have prefer not to answers did answer all the
# other questions
# So we're just keeping those in the dataset with null values
# nulls should propogate through addition

# combine questionnaires
for idx, (original_field, followup_field) in enumerate(zip(original_fields, followup_fields)):
    df = df.with_columns(
        pl.when(
            ~pl.col(original_field).is_null()
        ).then(
            pl.col(original_field)
        ).otherwise(
            pl.col(followup_field)
        ).alias(f'combined_q{idx+1}'),
        pl.when(
            ~pl.col(original_field).is_null()
        ).then(
            0
        ).when(
            ~pl.col(followup_field).is_null()
        ).then(
            1
        ).otherwise(
            None
        ).alias(f'combined_q{idx+1}_is_from_followup'),
        pl.when(
            ~pl.col(original_field).is_null()
        ).then(
            pl.col('original_rough_age_in_days')
        ).when(
            ~pl.col(followup_field).is_null()
        ).then(
            pl.col('followup_rough_age_in_days')
        ).otherwise(
            None
        ).alias(f'combined_q{idx+1}_rough_age_in_days'),
    )

combined_fields = [f'combined_q{idx}' for idx in range(1, 11)]

## calculate audit composite scores
sum_fields = ['c', 'p', 't']
log_fields = [f'{field}_log10' for field in sum_fields]
for fields, prefix in (original_fields, 'original'), (followup_fields, 'followup'):
    df = df.with_columns(
        functools.reduce(operator.add, [pl.col(field) for field in fields[:3]]).alias(f'{prefix}_c'),
        functools.reduce(operator.add, [pl.col(field) for field in fields[3:]]).alias(f'{prefix}_p'),
        functools.reduce(operator.add, [pl.col(field) for field in fields]).alias(f'{prefix}_t'),
    ).with_columns(
        (pl.col(f'{prefix}_{sum_field}') + 1).log10().alias(f'{prefix}_{log_field}') for sum_field, log_field in zip(sum_fields, log_fields)
    )

for fields_subset, indicator_subset, name in [
    (combined_fields[:3], [f'combined_q{idx}_is_from_followup' for idx in range(1, 4)], 'combined_c'),
    (combined_fields[3:], [f'combined_q{idx}_is_from_followup' for idx in range(4, 11)], 'combined_p'),
    (combined_fields, [f'combined_q{idx}_is_from_followup' for idx in range(1, 11)], 'combined_t')
]:
    df = df.with_columns(
        pl.when(
            functools.reduce(operator.add, [pl.col(field) for field in indicator_subset]).is_in([0, len(fields_subset)])
        ).then(
            functools.reduce(operator.add, [pl.col(field) for field in fields_subset]),
        ).otherwise(
            None
        ).alias(name),
        pl.when(
            functools.reduce(operator.add, [pl.col(field) for field in indicator_subset]) == 0
        ).then(
            pl.col('original_rough_age_in_days')
        ).when(
            functools.reduce(operator.add, [pl.col(field) for field in indicator_subset]) == len(fields_subset)
        ).then(
            pl.col('followup_rough_age_in_days')
        ).otherwise(
            None
        ).alias(f'{name}_rough_age_in_days'),
        pl.when(
            functools.reduce(operator.add, [pl.col(field) for field in indicator_subset]) == 0
        ).then(
            0
        ).when(
            functools.reduce(operator.add, [pl.col(field) for field in indicator_subset]) == len(fields_subset)
        ).then(
            1
        ).otherwise(
            None
        ).alias(f'{name}_is_from_followup')
    ).with_columns(
        (pl.col(name) + 1).log10().alias(f'{name}_log10')
    )

## make binary fields
df = df.with_columns(
    (pl.col(field) > 0).cast(int).alias(f'{field}_binarized')
    for field in [*original_fields[3:], *followup_fields[3:], *combined_fields[3:], 'original_p', 'followup_p', 'combined_p']
)

df.write_csv(
    f'{args.outdir}/full_audit.tab',
    separator='\t'
)

for prefix in 'original', 'followup':
    for field in [
        *[f'{prefix}_q{idx}' for idx in range(1, 11)],
        *[f'{prefix}_q{idx}_binarized' for idx in range(4, 11)],
        f'{prefix}_c',
        f'{prefix}_p',
        f'{prefix}_p_binarized',
        f'{prefix}_t',
        f'{prefix}_c_log10',
        f'{prefix}_p_log10',
        f'{prefix}_t_log10'
    ]:
        temp_df = df.select(
            'ID',
            field,
            f'{prefix}_rough_age_in_days'
        ).filter(
            ~pl.col(field).is_null()
        )
        assert ~temp_df.select(pl.any_horizontal(pl.col('*').is_null().any())).item()
        np.save(
            f'{args.outdir}/{field}',
            temp_df.to_numpy()
        )

# duplicate these columns with new names
# just so the next loop is simpler to write
df = df.with_columns(
    *[pl.col(f'{field}_rough_age_in_days').alias(f'{field}_log10_rough_age_in_days') for field in ('combined_c', 'combined_p', 'combined_t')],
    *[pl.col(f'{field}_is_from_followup').alias(f'{field}_log10_is_from_followup') for field in ('combined_c', 'combined_p', 'combined_t')],
    *[pl.col(f'{field}_rough_age_in_days').alias(f'{field}_binarized_rough_age_in_days') for field in [*combined_fields, 'combined_p']],
    *[pl.col(f'{field}_is_from_followup').alias(f'{field}_binarized_is_from_followup') for field in [*combined_fields, 'combined_p']]
)

for field in [
    *[f'combined_q{idx}' for idx in range(1, 11)],
    *[f'combined_q{idx}_binarized' for idx in range(4, 11)],
    'combined_c',
    'combined_p',
    'combined_p_binarized',
    'combined_t',
    'combined_c_log10',
    'combined_p_log10',
    'combined_t_log10'
]:
    temp_df = df.select(
        'ID',
        field,
        f'{field}_rough_age_in_days',
        f'{field}_is_from_followup'
    ).filter(
        ~pl.col(field).is_null()
    )
    assert not temp_df.select(pl.any_horizontal(pl.col('*').is_null().any())).item()

    np.save(
        f'{args.outdir}/{field}',
        temp_df.to_numpy()
    )

