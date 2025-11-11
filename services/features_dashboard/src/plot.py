from bokeh.plotting import figure
from bokeh.models import ColumnDataSource, HoverTool, NumeralTickFormatter, BoxAnnotation
import pandas as pd

def plot_data(
    data,
    title="OHLC Candlestick Chart",
    width=1200,
    height=600,
):
    df = data.copy()
    
    # --- Convert timestamp ---
    ts = df["timestamp"]
    if ts.max() > 10**12:
        df["date"] = pd.to_datetime(ts, unit="ms")
    else:
        df["date"] = pd.to_datetime(ts, unit="s")
    
    df = df.sort_values("date").reset_index(drop=True)

    # --- Full source for hover & wicks ---
    source = ColumnDataSource(df)

    # --- Separate sources for up/down candles (required in Bokeh 2.x) ---
    inc = df.close > df.open
    dec = df.open > df.close

    source_inc = ColumnDataSource(df[inc])
    source_dec = ColumnDataSource(df[dec])

    # --- Hover tool ---
    hover = HoverTool(tooltips=[
        ("Time",   "@date{%F %H:%M}"),
        ("Open",   "$@open{0,0.00}"),
        ("High",   "$@high{0,0.00}"),
        ("Low",    "$@low{0,0.00}"),
        ("Close",  "$@close{0,0.00}"),
    ], formatters={'@date': 'datetime'})

    # --- Figure ---
    p = figure(
        x_axis_type="datetime",
        tools="pan,wheel_zoom,box_zoom,reset,save",
        width=width, height=height,
        title=title,
        background_fill_color="#1e1e1e",
        border_fill_color="#1e1e1e",
    )
    p.add_tools(hover)

    # --- USD formatting ---
    p.yaxis.formatter = NumeralTickFormatter(format="$0,0.00")
    p.yaxis.major_label_text_color = "#cccccc"
    p.xaxis.major_label_text_color = "#cccccc"
    p.title.text_color = "#ffffff"
    p.grid.grid_line_color = "#333333"
    p.xaxis.major_label_orientation = 0.8

    # --- Weekend shading ---
    diff = df['date'].diff()
    big_gaps = diff[diff > pd.Timedelta('1D')]
    if len(big_gaps) > 0:
        gap_starts = df['date'].iloc[big_gaps.index - 1]
        gap_ends = df['date'].iloc[big_gaps.index]
        boxes = [BoxAnnotation(left=s, right=e, fill_color="#333333", fill_alpha=0.3)
                 for s, e in zip(gap_starts, gap_ends)]
        p.renderers.extend(boxes)

    # --- Dynamic candle width ---
    interval_sec = diff.dt.total_seconds().mode()
    interval_sec = interval_sec[0] if len(interval_sec) > 0 else 60
    candle_width = pd.Timedelta(seconds=interval_sec * 0.8)

    # === Wicks (from full source) ===
    p.segment(
        x0='date', y0='high',
        x1='date', y1='low',
        source=source,
        color="white",
        line_width=1,
    )

    # === Decreasing candles (red) ===
    p.vbar(
        x='date', width=candle_width,
        top='open', bottom='close',
        source=source_dec,           # ← only down candles
        fill_color="#e74c3c",
        line_color="#e74c3c",
        line_width=1,
    )

    # === Increasing candles (green) ===
    p.vbar(
        x='date', width=candle_width,
        top='close', bottom='open',
        source=source_inc,           # ← only up candles
        fill_color="#26a69a",
        line_color="#26a69a",
        line_width=1,
    )

    p.output_backend = "webgl"
    return p