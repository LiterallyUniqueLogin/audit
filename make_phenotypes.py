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
)

# commpute ages
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
    ).alias('rough_original_age_in_days'),
    pl.when(
        ~pl.col('29202').is_null() & (pl.col('29202') != '')
    ).then(
        pl.col('29202').str.to_date('%Y-%m-%d').cast(int) - pl.col('rough_birthdate_int')
    ).otherwise(
        None
    ).alias('rough_followup_age_in_days')
)

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

# set prefer not to answer to nones
for field in original_fields:
    df = df.with_columns([pl.when(pl.col(field) == -818).then(None).otherwise(pl.col(field)).alias(field)])
for field in followup_fields:
    df = df.with_columns([pl.when(pl.col(field) == -3).then(None).otherwise(pl.col(field)).alias(field)])

#non_gating_r2s = []
#non_gating_ns = []
#for q_num, (original_field, followup_field) in enumerate(list(zip(original_fields, followup_fields))):
#    q_num += 1
#    q_df = df.filter(
#        ~pl.col(original_field).is_null() &
#        ~pl.col(followup_field).is_null()
#    )
#    non_gating_ns.append(q_df.shape[0])
#
#    ori = q_df.select(original_field).to_numpy().flatten()
#    follow = q_df.select(followup_field).to_numpy().flatten()
#    non_gating_r2s.append(np.corrcoef(ori, follow)[0, 1]**2)

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

for q_num, (original_field, followup_field, non_gating_r2, non_gating_n) in enumerate(list(zip(original_fields, followup_fields, non_gating_r2s, non_gating_ns))):
    q_num += 1
    q_df = df.filter(
        ~pl.col(original_field).is_null() &
        ~pl.col(followup_field).is_null()
    )

    print(f'Q: {q_num} gating_n {q_df.shape[0]} non_gating n {non_gating_n}')
    ori = q_df.select(original_field).to_numpy().flatten()
    follow = q_df.select(followup_field).to_numpy().flatten()
    gating_r2 = np.corrcoef(ori, follow)[0, 1]**2
    print(f'Gating r2: {gating_r2:.2f} Non-gating r2: {non_gating_r2:.2f}')
    print()
exit()

# did not remove individuals with prefer not to answers. Simply keeping them in, just with nulls
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
        ).alias(f'is_combined_q{idx+1}_from_followup'),
        pl.when(
            ~pl.col(original_field).is_null()
        ).then(
            pl.col('rough_original_age_in_days')
        ).when(
            ~pl.col(followup_field).is_null()
        ).then(
            pl.col('rough_followup_age_in_days')
        ).otherwise(
            None
        ).alias(f'rough_age_in_days_at_combined_q{idx+1}'),
    )

## calculate audit composite scores
#audit_sum_fields = ['audit_c', 'audit_p', 'audit_t']
#audit_log_fields = [f'{field}_log10' for field in audit_sum_fields]
#df = df.with_columns([
#    functools.reduce(operator.add, [pl.col(field) for field in fields[:3]]).alias('audit_c'),
#    functools.reduce(operator.add, [pl.col(field) for field in fields[3:]]).alias('audit_p'),
#    functools.reduce(operator.add, [pl.col(field) for field in fields]).alias('audit_t'),
#]).with_columns([
#    (pl.col(sum_field) + 1).log10().alias(log_field) for sum_field, log_field in zip(audit_sum_fields, audit_log_fields)
#])

#df = df.filter(
#    pl.any_horizontal(~pl.all().exclude('eid').is_null())
#)
#print('N at least one response', df.shape[0])

df.rename(
    {'eid': 'ID'}
).write_csv(
    f'{args.outdir}/full_audit.tab',
    separator='\t'
)

for idx in range(10):
    temp_df = df.select(
        'eid',
        f'combined_q{idx+1}',
        f'rough_age_in_days_at_combined_q{idx+1}',
        f'is_combined_q{idx+1}_from_followup'
    ).filter(
        ~pl.col(f'combined_q{idx+1}').is_null()
    )
    assert ~temp_df.select(pl.any_horizontal(pl.all().is_null().any())).item()
    np.save(
        f'{args.outdir}/audit_q{idx+1}',
        temp_df.to_numpy()
    )

#for field in audit_sum_fields + audit_log_fields:
#    np.save(
#        f'{args.outdir}/{field}',
#        df.select(
#            'eid',
#            field,
#            'rough_age_in_days'
#        ).filter(
#            ~pl.col(field).is_null()
#        ).to_numpy()
#    )
