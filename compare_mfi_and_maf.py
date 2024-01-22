import os

import polars as pl

ukb = os.environ['UKB']

mafs = pl.scan_csv(
    'data/gwas_results/q4/mafs.tab',
    separator='\t',
).filter(
    pl.col('ALT_FREQS') > 0.0005
).filter(
    pl.col('#CHROM').is_in(list(range(1, 23)))
).with_columns(
    pl.col('#CHROM').cast(int)
).collect()
print(mafs.shape)

infos_per_chrom = [pl.read_csv(
    f'{ukb}/array_imputed/ukb_mfi_chr{chrom}_v3.txt',
    has_header = False,
    new_columns = ['combined_ID', 'ID', 'pos', 'ref', 'alt', 'maf', '?', 'info'],
    separator='\t',
    null_values='NA'
).with_columns(pl.lit(chrom).alias('chrom').cast(int)) for chrom in range(1, 23)]

infos = infos_per_chrom[0]
for other_infos in infos_per_chrom[1:]:
    infos = infos.vstack(other_infos)

print('mafs', *zip(mafs.columns, mafs.dtypes))
print('infos', *zip(infos.columns, infos.dtypes))

total = mafs.join(
    infos,
    how='left',
    left_on = ['#CHROM', 'POS', 'REF', 'ALT'],
    right_on = ['chrom', 'pos', 'ref', 'alt']
)

assert ~total.select(pl.col('info').is_null().any()).item()

print(
    total.select(pl.col('info').min())
)
