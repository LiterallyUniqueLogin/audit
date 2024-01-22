import numpy as np
import polars as pl

import bokeh.plotting
import bokeh.models
import bokeh.io
import math

chr_lens = np.genfromtxt(
    '../ukbiobank/misc_data/genome/chr_lens.txt',
    usecols=[1],
    skip_header=1,
    dtype=int
)

#for id_, name in (20413, 'q4'), (20416, 'q3'):
for id_, name in [(20407, 'q5')]:
    df = pl.scan_csv(
        f'data/pan_ukbb/continuous-{id_}-both_sexes.tsv',
        separator='\t',
        null_values='NA',
        dtypes = {'chr': str}
    ).filter(
        pl.col('chr').is_in([str(val) for val in range(1, 23)])
    ).select(
        'neglog10_pval_EUR',
        pl.col('chr').cast(int),
        'pos'
    ).filter(
        ~pl.col('neglog10_pval_EUR').is_null() &
        (pl.col('neglog10_pval_EUR') > 3)
    ).with_columns([
        pl.col('pos').alias('plot_pos'),
        pl.when(
            pl.col('neglog10_pval_EUR') < -np.log10(5e-8)
        ).then(
            pl.lit('#A9A9A9')
        ).otherwise(
            pl.lit('#00B8FF')
        ).alias('color')
    ])
    for chrom in range(2, 23):
        df = df.with_columns([pl
            .when(pl.col('chr') >= chrom)
            .then(pl.col('plot_pos') + int(chr_lens[chrom - 2]))
            .otherwise(pl.col('plot_pos'))
            .alias('plot_pos')
        ])

    df = df.collect()
    data = {k: v.to_numpy() for (k, v) in df.to_dict().items()}
    manhattan_plot = bokeh.plotting.figure(
        width=math.floor(4.25*400),
        height=400,
        title=f'Pan ukbb Manhattan {name}',
        x_axis_label='Chromosomes',
        y_axis_label='-log10(p-value)',
    )

    manhattan_plot.title.text_font_size = '30px'
    manhattan_plot.axis.axis_label_text_font_size = '26px'
    manhattan_plot.axis.major_label_text_font_size = '20px'
    manhattan_plot.grid.grid_line_color = None
    manhattan_plot.background_fill_color = None
    manhattan_plot.border_fill_color = None
    manhattan_plot.toolbar_location = None

    pre_chr_sums = np.cumsum([0, *chr_lens[:-1]])
    mid_points = [int(num) for num in pre_chr_sums + (chr_lens//2)]
    manhattan_plot.xaxis.ticker = mid_points
    manhattan_plot.xaxis.major_label_overrides = {
            mid_points[chrom - 1]: str(chrom) for chrom in range(1, 23)
    }

    manhattan_plot.diamond(
        'plot_pos',
        'neglog10_pval_EUR',
        source=bokeh.models.ColumnDataSource(data),
        color='color',
        size=8
    )
    bokeh.io.export_png(manhattan_plot, filename=f'data/pan_ukbb/{name}_manhattan.png')

