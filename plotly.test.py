import plotly
from plotly.offline import plot as offpy
import plotly.graph_objs as go

def bar_chart_plot(x, y, title):
    data = [ go.Bar(
        x=x,
        y=y
    ) ]
    layout = go.Layout(title=title)
    fig = go.Figure(data=data, layout=layout) 
    return offpy(fig,include_plotlyjs=False,show_link=False,output_type='div')

if __name__ == "__main__":
    x = [1,2,3]
    y = [10,11,2]
    bar_chart_plot(x,y,'random bar')
