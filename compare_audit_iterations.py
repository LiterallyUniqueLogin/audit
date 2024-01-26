import bokeh.io
import bokeh.models
import bokeh.plotting
import numpy as np
import polars as pl

def linear_int_interpolate(c1, c2, dist):
    c_new = []
    for coord1, coord2 in zip(c1, c2):
        c_new.append(coord1 + round((coord2 - coord1)*dist))
    return c_new


total_df = pl.read_csv(
    #'data/formatted_data/full_audit.tab',
    'data/new_problematic_formatted_data/full_audit.tab',
    separator='\t'
)

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

for idx, (original_field, followup_field) in enumerate(zip(original_fields, followup_fields)):
    df = total_df.filter(
        ~pl.col(original_field).is_null() &
        ~pl.col(followup_field).is_null()
    )
    n_only_ori = total_df.filter(
        ~pl.col(original_field).is_null() &
        pl.col(followup_field).is_null()
    ).shape[0]
    n_only_follow = total_df.filter(
        pl.col(original_field).is_null() &
        ~pl.col(followup_field).is_null()
    ).shape[0]

    if idx == 0:
        print(f'N shared {df.shape[0]}, n only original {n_only_ori} n only followup {n_only_follow}')
        print('r^2s between shared responses')
    print(f'Q{idx+1}', end='\t')
    ori = df.select(original_field).to_numpy().flatten()
    follow = df.select(followup_field).to_numpy().flatten()
    real_r2 = np.corrcoef(ori, follow)[0, 1]**2
    print(f'{real_r2:.2}')
    continue

    fig = bokeh.plotting.figure(
        title=f'AUDIT Q{idx+1} Followup vs Original questionnaire responses',
        y_axis_label = 'Followup response',
        x_axis_label = 'Original response',
        width=1200,
        height=1200,
        output_backend='svg'
    )

    fig.background_fill_color = None
    fig.border_fill_color = None
    fig.grid.grid_line_color = None
    fig.toolbar_location = None
    fig.title.text_font_size = '30px'
    fig.axis.axis_label_text_font_size = '26px'
    fig.axis.major_label_text_font_size = '20px'

    grid = np.mgrid[:5, :5]
    x = grid[0,:,:].flatten()
    y = grid[1,:,:].flatten()

    counts = [
        df.filter(
            (int(xval) <= pl.col(original_field)) &
            (pl.col(original_field) < int(xval) + 1) &
            (int(yval) <= pl.col(followup_field)) &
            (pl.col(followup_field) < int(yval) + 1)
        ).shape[0]
        for (xval, yval) in zip(x, y)
    ]

    cds = bokeh.models.ColumnDataSource(dict(
        left = x,
        right = x + 1,
        bottom = y,
        top = y + 1,
        counts = counts,
        str_counts = [str(count) for count in counts]
    ))

    palette = [
        linear_int_interpolate((134,204,195), (9,41,46), i/254) for i in range(-1, 255)
    ]
    cmap = bokeh.transform.log_cmap(
        'counts',
        palette = palette,
        low=1,
        high=np.max(cds.data['counts']),
        low_color=(255, 255, 255)
    )
    color_mapper = bokeh.models.LogColorMapper(
        palette = palette,
        low=1,
        high=np.max(cds.data['counts'])
    )

    fig.quad(
        left='left', right='right', bottom='bottom', top='top', source=cds, fill_color=cmap, line_width=0 # W: Line too long (105/100)
    )

    fig.add_layout(bokeh.models.LabelSet(
        x = 'left',
        y = 'bottom',
        text_font_size = '40px',
        text_color = 'black',
        text = 'str_counts',
        source = cds,
    ))

    color_bar = bokeh.models.ColorBar(
        color_mapper = color_mapper,
        width=70,
        major_label_text_font_size = '20px'
    )
    fig.add_layout(color_bar, 'right')

    #graphing_utils.resize(fig, 5000/1200, legend=False)
    bokeh.io.export_png(fig, filename=f'temp/q{idx+1}_followup_v_original.png')

