import pandas as pd

from bokeh.plotting import figure, show, output_file
from bokeh.models import ColumnDataSource, HoverTool, FactorRange, Select, CustomJS
from bokeh.layouts import column, row
from bokeh.transform import dodge
from bokeh.palettes import Category10


df = pd.read_csv('data/Titanic/Titanic-Dataset.csv')


def categorize_age(age):
    if age < 18:
        return 'Child'
    elif age < 30:
        return 'Young Adult'
    elif age < 60:
        return 'Adult'
    else:
        return 'Senior'


if __name__ == '__main__':
    #1 preprocessing
    median_age = df['Age'].median()
    mode_embarked = df['Embarked'].mode()[0]
    df['Age'].fillna(median_age, inplace=True)
    df['Embarked'].fillna(mode_embarked, inplace=True)
    df['Cabin'].fillna('Unknown', inplace=True)
    df['AgeGroup'] = df['Age'].apply(categorize_age)
    df['SurvivalRate'] = df.groupby('AgeGroup')['Survived'].transform('mean') * 100

    #2 visualizations
    output_file('titanic_visualizations.html')

    ##`Age Group Survival`: Create a bar chart showing survival rates across different age groups.
    age_groups = df['AgeGroup'].unique()
    age_group_data = df.groupby('AgeGroup')['SurvivalRate'].mean().reindex(age_groups).values

    age_group_source = ColumnDataSource(data=dict(AgeGroup=age_groups, SurvivalRate=age_group_data))

    p1 = figure(x_range=age_groups, title="Survival Rates by Age Group",
            x_axis_label='Age Group', y_axis_label='Survival Rate (%)',
            height=400, width=1000, toolbar_location=None)
    p1.vbar(x='AgeGroup', top='SurvivalRate', width=0.9, source=age_group_source, color='navy')

    p1.add_tools(HoverTool(tooltips=[("Age Group", "@AgeGroup"), ("Survival Rate", "@SurvivalRate{0.2f}%")]))
    p1.xgrid.grid_line_color = None
    p1.y_range.start = 0
    p1.y_range.end = 100

    #`Class and Gender`: Create a grouped bar chart to compare survival rates across different classes (1st, 2nd, 3rd) and genders (male, female).
    class_gender_data = df.groupby(['Pclass', 'Sex'])['Survived'].mean().unstack().reset_index()
    class_gender_data.loc[:, ['female', 'male']] = class_gender_data.loc[:, ['female', 'male']]*100
    class_gender_source = ColumnDataSource(class_gender_data)
    p2 = figure(x_range=[f'Class {i}' for i in class_gender_data['Pclass'].unique()],
            title="Survival Rates by Class and Gender",
            x_axis_label='Class', y_axis_label='Survival Rate (%)',
            height=400, width=1000, toolbar_location=None)
    p2.vbar(x=dodge('Pclass', -0.65,  range=p2.x_range), top='female', source=class_gender_source,
    width=0.25, color="#e84d60", legend_label="female")
    p2.vbar(x=dodge('Pclass',  -0.35,  range=p2.x_range), top='male', source=class_gender_source,
    width=0.25, color="#718dbf", legend_label="male")
    p2.xgrid.grid_line_color = None
    p2.y_range.start = 0
    p2.y_range.end = 100
    p2.legend.title = 'Gender'
    p2.legend.orientation = "horizontal"
    p2.legend.location = "top_right"

    # `Fare vs. Survival`: Create a scatter plot with Fare on the x-axis and survival status on the y-axis, using different colors to represent different classes.
    fare_survival_source = ColumnDataSource(df)
    color_map = {1: Category10[3][0], 2: Category10[3][1], 3: Category10[3][2]}
    fare_survival_source.data['color'] = [color_map[cls] for cls in fare_survival_source.data['Pclass']]
    p3 = figure(title="Fare vs. Survival",
            x_axis_label='Fare', y_axis_label='Survived',
            tools='pan,box_zoom,reset,save')
    p3.scatter(x='Fare', y='Survived', color='color', source=fare_survival_source,
            legend_group='Pclass', fill_alpha=0.6, size=8)
    p3.add_tools(HoverTool(tooltips=[("Fare", "@Fare"), ("Survived", "@Survived"), ("Class", "@Pclass")]))
    p3.y_range.start = -0.1
    p3.y_range.end = 1.1
    p3.legend.location = "bottom_right"

    layout = column(p1, p2, p3)
    show(layout)
