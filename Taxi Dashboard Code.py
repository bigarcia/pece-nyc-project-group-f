import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html, Input, Output
from sqlalchemy import create_engine
import threading
import webbrowser

# Conexão com banco de dados RDS
ENDPOINT = "nyc-dw-mysql.cwr8bgukmana.us-east-1.rds.amazonaws.com"
PORT = "3306"
USER = "admin"
PASSWORD = "passwordrds1"
DBNAME = "nyc_dw"
engine = create_engine(f"mysql+mysqlconnector://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DBNAME}")

# ------------------------- Dados da TELA 1 -------------------------
query1 = """
SELECT
    f.pickup_datetime,
    YEAR(f.pickup_datetime) AS year,
    MONTH(f.pickup_datetime) AS month,
    DAYNAME(f.pickup_datetime) AS day_of_week,
    HOUR(f.pickup_datetime) AS pickup_hour,
    f.fk_service_type AS service_type,
    f.sk_trip AS trip_id
FROM fact_taxi_trip f
WHERE YEAR(f.pickup_datetime) IN (2022, 2023, 2024)
"""
df1 = pd.read_sql(query1, engine)
df1 = df1.dropna(subset=['month', 'day_of_week', 'pickup_hour', 'service_type'])
df1['month'] = df1['month'].astype(int)
df1['pickup_hour'] = df1['pickup_hour'].astype(int)
df1['year'] = df1['year'].astype(int)
df1['service_type'] = df1['service_type'].astype(str)

dias_semana_map = {
    'Monday': 'Segunda-feira', 'Tuesday': 'Terça-feira', 'Wednesday': 'Quarta-feira',
    'Thursday': 'Quinta-feira', 'Friday': 'Sexta-feira', 'Saturday': 'Sábado', 'Sunday': 'Domingo'
}
df1['day_of_week'] = df1['day_of_week'].map(dias_semana_map)

# ------------------------- Dados da TELA 2 -------------------------
query2 = """
SELECT
    f.pickup_datetime,
    f.dropoff_datetime,
    f.trip_distance,
    f.total_amount,
    f.fk_service_type AS service_type,
    f.id_location_pickup,
    l.zone AS region
FROM fact_taxi_trip f
LEFT JOIN dim_location l ON f.id_location_pickup = l.id_location
WHERE YEAR(f.pickup_datetime) IN (2022, 2023, 2024)
  AND f.total_amount > 0
  AND f.trip_distance > 0
  AND f.dropoff_datetime > f.pickup_datetime
"""
df2 = pd.read_sql(query2, engine)
df2['pickup_datetime'] = pd.to_datetime(df2['pickup_datetime'])
df2['dropoff_datetime'] = pd.to_datetime(df2['dropoff_datetime'])
df2['year'] = df2['pickup_datetime'].dt.year
df2['month'] = df2['pickup_datetime'].dt.month
df2['duration'] = (df2['dropoff_datetime'] - df2['pickup_datetime']).dt.total_seconds() / 60
df2 = df2[df2['duration'] > 0]
df2['valor_por_corrida'] = df2['total_amount']
df2['valor_por_hora'] = df2['total_amount'] / (df2['duration'] / 60)
tipos_conhecidos = ['yellowTaxi', 'greenTaxi', 'forHireVehicle', 'highVolumeForHire']

# ------------------------- Dados da TELA 3 -------------------------
query3 = """
SELECT
    f.pickup_datetime,
    f.dropoff_datetime,
    f.fk_service_type AS service_type,
    l1.zone AS pickup_zone,
    l2.zone AS dropoff_zone
FROM fact_taxi_trip f
LEFT JOIN dim_location l1 ON f.id_location_pickup = l1.id_location
LEFT JOIN dim_location l2 ON f.id_location_dropoff = l2.id_location
WHERE f.pickup_datetime IS NOT NULL AND f.dropoff_datetime IS NOT NULL
"""

# Executa a query SQL e carrega o resultado em um DataFrame
df3 = pd.read_sql(query3, engine)
df3['pickup_datetime'] = pd.to_datetime(df3['pickup_datetime']) # Converte colunas de data/hora para o formato datetime do pandas
df3['dropoff_datetime'] = pd.to_datetime(df3['dropoff_datetime'])
df = df3[df3['dropoff_datetime'] > df3['pickup_datetime']] # Remove corridas inválidas com datas invertidas

# Criação de colunas derivadas para análises posteriores
df3['year'] = df3['pickup_datetime'].dt.year                          # Extrai o ano3
df3['hour'] = df3['pickup_datetime'].dt.hour                          # Extrai a hora
df3['day_of_week'] = df3['pickup_datetime'].dt.day_name()             # Nome do dia da semana
df3['season'] = df3['pickup_datetime'].dt.month % 12 // 3 + 1         # Estação em número (1 a 4)

# Mapeamento das estações do ano3
season_map = {1: 'Verão', 2: 'Outono', 3: 'Inverno', 4: 'Primavera'}
df3['season'] = df3['season'].map(season_map)                         # Substitui número por nome da estação

# ------------------------- Dados da TELA 4 -------------------------
# Consulta SQL que retorna corridas com valores válidos para tarifa e gorjeta
query4 = """
SELECT
    pickup_datetime,
    dropoff_datetime,
    fare_amount,
    tip_amount,
    trip_distance,
    fk_service_type
FROM fact_taxi_trip
WHERE pickup_datetime IS NOT NULL AND dropoff_datetime IS NOT NULL
  AND fare_amount > 0 AND tip_amount >= 0
  AND dropoff_datetime > pickup_datetime
"""

# Executa a query SQL e carrega os dados em um DataFrame do pandas
df4 = pd.read_sql(query4, engine)
df4['pickup_datetime'] = pd.to_datetime(df4['pickup_datetime'])
df4['dropoff_datetime'] = pd.to_datetime(df4['dropoff_datetime'])
df4 = df4[df4['dropoff_datetime'] > df4['pickup_datetime']]
df4['year'] = df4['pickup_datetime'].dt.year
df4['hour'] = df4['pickup_datetime'].dt.hour
df4['day_of_week'] = df4['pickup_datetime'].dt.day_name()
df4['season'] = df4['pickup_datetime'].dt.month % 12 // 3 + 1
season_map = {1: 'Verão', 2: 'Outono', 3: 'Inverno', 4: 'Primavera'}
df4['season'] = df4['season'].map(season_map)

# Converte colunas de data e hora para o tipo datetime do pandas
df4['pickup_datetime'] = pd.to_datetime(df4['pickup_datetime'])
df4['dropoff_datetime'] = pd.to_datetime(df4['dropoff_datetime'])

# Calcula a duração da corrida em minutos
df4['trip_duration'] = (df4['dropoff_datetime'] - df4['pickup_datetime']).dt.total_seconds() / 60

# Extrai ano e mês da data de embarque
df4['year'] = df4['pickup_datetime'].dt.year
df4['month'] = df4['pickup_datetime'].dt.month

# Calcula a estação do ano com base no mês
df4['season'] = df4['pickup_datetime'].dt.month % 12 // 3 + 1

# Mapeia a estação numérica para o nome da estação (pt-BR)
df4['season'] = df4['season'].map({1: 'Verão', 2: 'Outono', 3: 'Inverno', 4: 'Primavera'})


# ------------------------- Criação do App -------------------------
app = Dash(__name__)
app.config.suppress_callback_exceptions = True
app.title = "Dashboard Táxi Unificado"

app.layout = html.Div([
    html.H1("Dashboard Táxi NYC", style={'textAlign': 'center'}),
    dcc.Tabs(id='tabs', value='tela1', children=[
        dcc.Tab(label='Tela 1 - Tendências de Demanda', value='tela1'),
        dcc.Tab(label='Tela 2 - Eficiência Operacional', value='tela2'),
        dcc.Tab(label='Tela 3 - Hotspots (origem e destino como mudaram ao longo do tempo)', value='tela3'),
        dcc.Tab(label='Tela 4 - Análise Tarifas e Gorjetas', value='tela4')
    ]),
    html.Div(id='conteudo')
])  

# ------------------------- Layouts Separados -------------------------
# ------------------------- Layout TELA 1 -------------------------
layout_tela1 = html.Div([
    html.Div([
        dcc.Dropdown(id='ano1', options=[{'label': str(y), 'value': y} for y in [2022, 2023, 2024]],
                     placeholder='Ano', clearable=True, style={'width': '140px'}),
        dcc.Dropdown(id='mes1', options=[{'label': m, 'value': v} for v, m in enumerate([
            'Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
            'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'], 1)],
                     placeholder='Mês', clearable=True, style={'width': '150px'}),
        dcc.Dropdown(id='dia1', options=[{'label': d, 'value': d} for d in dias_semana_map.values()],
                     placeholder='Dia da Semana', clearable=True, style={'width': '180px'}),
        dcc.Dropdown(id='hora1', options=[{'label': f"{h:02d}h", 'value': h} for h in sorted(df1['pickup_hour'].unique())],
                     placeholder='Hora do Dia', clearable=True, style={'width': '140px'}),
        dcc.Dropdown(id='servico1', options=[{'label': s, 'value': s} for s in sorted(df1['service_type'].unique())],
                     placeholder='Tipo de Serviço', clearable=True, style={'width': '180px'}),
    ], style={'display': 'flex', 'gap': '10px', 'flex-wrap': 'wrap', 'margin-bottom': '20px'}),
    html.Div([
        html.Div([dcc.Graph(id='grafico_temporal')], style={'width': '49%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(id='grafico_hora')], style={'width': '49%', 'display': 'inline-block'})
    ]),
    html.Div([dcc.Graph(id='grafico_servico')])
])

# ------------------------- Layout TELA 2 -------------------------
layout_tela2 = html.Div([
    html.Div([
        dcc.Dropdown(id='ano2',
                     options=[{'label': str(y), 'value': y} for y in sorted(df2['year'].unique())],
                     value=2024, placeholder='Ano', style={'width': '120px'}),
        dcc.Dropdown(id='mes2',
                     options=[{'label': str(m), 'value': m} for m in sorted(df2['month'].unique())],
                     placeholder='Mês', style={'width': '120px'}),
        dcc.Dropdown(id='servico2',
                     options=[{'label': str(s), 'value': s} for s in sorted(df2['service_type'].unique())],
                     placeholder='Tipo de Serviço', style={'width': '180px'}),
        dcc.Dropdown(id='regiao2',
                     options=[{'label': str(r), 'value': r} for r in sorted(df2['region'].dropna().unique())],
                     placeholder='Zona de Embarque', style={'width': '200px'}),
    ], style={'display': 'flex', 'gap': '15px', 'flex-wrap': 'wrap', 'margin-bottom': '20px'}),
    html.Div([
        html.Div(dcc.Graph(id='graf1'), style={'width': '49%', 'display': 'inline-block'}),
        html.Div(dcc.Graph(id='graf2'), style={'width': '49%', 'display': 'inline-block'}),
        html.Div(dcc.Graph(id='graf3'), style={'width': '49%', 'display': 'inline-block'}),
        html.Div(dcc.Graph(id='graf4'), style={'width': '49%', 'display': 'inline-block'}),
    ]),
    html.Div(dcc.Graph(id='graf5'), style={'margin-top': '30px'}),
    html.Div(dcc.Graph(id='graf6'), style={'margin-top': '30px'}),
])

# ------------------------- Layout TELA 3 -------------------------
layout_tela3 = html.Div([
    html.Div([
        dcc.Dropdown(
            id='ano3',
            options=[{'label': str(y), 'value': y} for y in sorted(df3['year'].dropna().unique())],
            value=2024,
            placeholder="Ano", style={'width': '120px'}
        ),
        dcc.Dropdown(
            id='estacao3',
            options=[{'label': str(e), 'value': e} for e in sorted(df3['season'].dropna().unique())],
            placeholder="Estação do Ano", style={'width': '160px'}
        ),
        dcc.Dropdown(
            id='servico3',
            options=[{'label': str(s), 'value': s} for s in sorted(df3['service_type'].dropna().unique())],
            placeholder="Tipo de Serviço", style={'width': '180px'}
        )
    ], style={'display': 'flex', 'gap': '15px', 'flex-wrap': 'wrap', 'margin': '20px'}),

    html.Div([
        html.Div([dcc.Graph(id='graf13')], style={'width': '48%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(id='graf23')], style={'width': '48%', 'display': 'inline-block'})
    ]),
    html.Div([
        html.Div([dcc.Graph(id='graf33')], style={'width': '48%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(id='graf43')], style={'width': '48%', 'display': 'inline-block'})
    ])
])

# ------------------------- Layout TELA 4 -------------------------
layout_tela4 = html.Div([
    html.H1("Análise de Tarifas e Gorjetas"),  # Título do painel

    # Filtros interativos
    html.Div([
        dcc.Dropdown(
            id='ano',
            options=[{'label': str(y), 'value': y} for y in [2022, 2023, 2024]],
            placeholder='Ano', clearable=True, style={'width': '200px'}
        ),
        dcc.Dropdown(
            id='estacao',
            options=[{'label': s, 'value': s} for s in ['Verão', 'Outono', 'Inverno', 'Primavera']],
            placeholder='Estação do Ano', clearable=True, style={'width': '200px'}
        ),
        dcc.Dropdown(
            id='servico',
            options=[{'label': s, 'value': s} for s in sorted(df4['fk_service_type'].dropna().unique())],
            placeholder='Tipo de Serviço', clearable=True, style={'width': '200px'}
        )
    ], style={'display': 'flex', 'gap': '15px', 'flex-wrap': 'wrap', 'margin-bottom': '30px'}),

    # Área de gráficos
    html.Div([
        html.Div(dcc.Graph(id='grafico_tarifa_mes'), style={'width': '48%'}),
        html.Div(dcc.Graph(id='grafico_gorjeta_mes'), style={'width': '48%'}),
        html.Div(dcc.Graph(id='grafico_duracao_vs_gorjeta'), style={'width': '48%'}),
        html.Div(dcc.Graph(id='grafico_gorjeta_servico'), style={'width': '48%'}),
        html.Div(dcc.Graph(id='grafico_distancia_vs_gorjeta'), style={'width': '100%'})
    ], style={'display': 'flex', 'flex-wrap': 'wrap', 'gap': '20px'})
])



# ------------------------- Callback de troca de layout -------------------------
@app.callback(Output('conteudo', 'children'), Input('tabs', 'value'))
def render_layout(tab):
    if tab == 'tela1':
        return layout_tela1
    elif tab == 'tela2':
        return layout_tela2
    elif tab == 'tela3':
        return layout_tela3
    elif tab == 'tela4':
        return layout_tela4
    
# ------------------------- Callbacks TELA 1 -------------------------
@app.callback(
    Output('grafico_temporal', 'figure'),
    Output('grafico_hora', 'figure'),
    Output('grafico_servico', 'figure'),
    Input('ano1', 'value'), Input('mes1', 'value'), Input('dia1', 'value'),
    Input('hora1', 'value'), Input('servico1', 'value'),
    #prevent_initial_call=True
)
def update_tela1(ano, mes, dia, hora, servico):
    dff = df1.copy()
    if ano: dff = dff[dff['year'] == ano]
    if mes: dff = dff[dff['month'] == mes]
    if dia: dff = dff[dff['day_of_week'] == dia]
    if hora: dff = dff[dff['pickup_hour'] == hora]
    if servico: dff = dff[dff['service_type'] == servico]

    df_mes = dff.groupby('month').size().reset_index(name='corridas')
    df_mes['dimensao'] = 'Mês'
    df_mes.rename(columns={'month': 'eixo'}, inplace=True)

    df_dia = dff.groupby('day_of_week').size().reset_index(name='corridas')
    dias_ordenados = ['Segunda-feira', 'Terça-feira', 'Quarta-feira', 'Quinta-feira', 'Sexta-feira', 'Sábado', 'Domingo']
    df_dia = df_dia.set_index('day_of_week').reindex(dias_ordenados, fill_value=0).reset_index()
    df_dia['dimensao'] = 'Dia da Semana'
    df_dia.rename(columns={'day_of_week': 'eixo'}, inplace=True)

    df_temporal = pd.concat([df_mes, df_dia])
    # Criar eixo único com valores categóricos (meses + dias)
    meses_abrev = {
        1: 'Jan', 2: 'Fev', 3: 'Mar', 4: 'Abr', 5: 'Mai', 6: 'Jun',
        7: 'Jul', 8: 'Ago', 9: 'Set', 10: 'Out', 11: 'Nov', 12: 'Dez'
    }
    df_mes = dff.groupby('month').size().reset_index(name='corridas')
    df_mes['eixo'] = df_mes['month'].map(meses_abrev)
    df_mes['dimensao'] = 'Mês'

    traducao_dias = {
        'Monday': 'Segunda-feira',
        'Tuesday': 'Terça-feira',
        'Wednesday': 'Quarta-feira',
        'Thursday': 'Quinta-feira',
        'Friday': 'Sexta-feira',
        'Saturday': 'Sábado',
        'Sunday': 'Domingo'
    }
    dias_ordenados = ['Segunda-feira', 'Terça-feira', 'Quarta-feira', 'Quinta-feira', 'Sexta-feira', 'Sábado', 'Domingo']
    df_dia = dff.groupby('day_of_week').size().reset_index(name='corridas')
    df_dia['day_of_week'] = df_dia['day_of_week'].map(traducao_dias)
    df_dia = df_dia.groupby('day_of_week')['corridas'].sum().reset_index()
    df_dia = df_dia.set_index('day_of_week').reindex(dias_ordenados, fill_value=0).reset_index()
    df_dia['eixo'] = df_dia['day_of_week']
    df_dia['dimensao'] = 'Dia da Semana'

    # Juntar os dois
    df_temporal = pd.concat([df_mes[['eixo', 'corridas', 'dimensao']], df_dia[['eixo', 'corridas', 'dimensao']]])

    fig1 = px.line(
        df_temporal, x='eixo', y='corridas', color='dimensao',
        title='Corridas por Mês e Dia da Semana',
        markers=True,
        category_orders={'eixo': list(meses_abrev.values()) + dias_ordenados}
    ),
    xaxis=dict(
        title='Mês', domain=[0, 1], anchor='y'
    ),
    xaxis2=dict(
        title='Dia da Semana', domain=[0, 1], anchor='y2',
        overlaying='x', side='top'
    ),
    legend=dict(orientation='h', y=-0.2),
    height=500
    

    df_hora = dff.groupby('pickup_hour').size().reset_index(name='corridas')
    fig2 = px.bar(df_hora, x='pickup_hour', y='corridas', title='Corridas por Hora do Dia')

    df_serv = df1.copy()
    if ano: df_serv = df_serv[df_serv['year'] == ano]
    if servico: df_serv = df_serv[df_serv['service_type'] == servico]
    df_serv = df_serv.groupby(['month', 'service_type']).size().reset_index(name='corridas')
    fig3 = px.line(df_serv, x='month', y='corridas', color='service_type',
                   title='Corridas por Mês e Tipo de Serviço')

    return fig1, fig2, fig3


# ------------------------- Callbacks TELA 2 -------------------------
@app.callback(
    Output('graf1', 'figure'), Output('graf2', 'figure'), Output('graf3', 'figure'),
    Output('graf4', 'figure'), Output('graf5', 'figure'), Output('graf6', 'figure'),
    Input('ano2', 'value'), Input('mes2', 'value'),
    Input('servico2', 'value'), Input('regiao2', 'value'),
    #prevent_initial_call=True
)
def update_tela2(ano, mes, servico, regiao):
    dff = df2.copy()
    if ano: dff = dff[dff['year'] == ano]
    if mes: dff = dff[dff['month'] == mes]
    if servico: dff = dff[dff['service_type'] == servico]
    if regiao: dff = dff[dff['region'] == regiao]

    df1 = dff.groupby('month')['duration'].mean().reset_index()
    df2_ = dff.groupby('month')['trip_distance'].mean().reset_index()
    df3 = dff.groupby('month')['valor_por_corrida'].mean().reset_index()
    df4 = dff.groupby('month')['valor_por_hora'].mean().reset_index()

    df5 = df2[df2['year'] == ano] if ano else df2.copy()
    df5 = df5.groupby('service_type')['valor_por_hora'].mean().reindex(tipos_conhecidos).reset_index()
    df5['valor_por_hora'] = df5['valor_por_hora'].fillna(0)

    df6 = dff.groupby('region')['valor_por_hora'].mean().nlargest(10).reset_index()

    return (
        px.line(df1, x='month', y='duration', title='Tempo Médio por Mês (min)'),
        px.line(df2_, x='month', y='trip_distance', title='Distância Média por Corrida'),
        px.line(df3, x='month', y='valor_por_corrida', title='Faturamento Médio por Corrida'),
        px.line(df4, x='month', y='valor_por_hora', title='Faturamento Médio por Hora'),
        px.bar(df5, x='service_type', y='valor_por_hora', title='Faturamento por Tipo de Serviço'),
        px.bar(df6, x='region', y='valor_por_hora', title='Top 10 Regiões por Faturamento')
    )

# ------------------------- Callbacks TELA 3 -------------------------
@app.callback(
    Output('graf13', 'figure'), Output('graf23', 'figure'),
    Output('graf33', 'figure'), Output('graf43', 'figure'),
    Output('ano3', 'options'), Output('estacao3', 'options'), Output('servico3', 'options'),
    Input('ano3', 'value'), Input('estacao3', 'value'), Input('servico3', 'value'),
    #prevent_initial_call=True
)
def update_tela3(ano3, estacao3, servico3):
    dff = df3.copy()
    if ano3: dff = dff[dff['year'] == ano3]
    if estacao3: dff = dff[dff['season'] == estacao3]
    if servico3: dff = dff[dff['service_type'] == servico3]

    top_embarque = dff['pickup_zone'].value_counts().reset_index()
    top_embarque.columns = ['pickup_zone', 'count']
    top_embarque = top_embarque[top_embarque['count'] > 0].sort_values(by='count', ascending=False).head(10)
    fig1 = px.bar(top_embarque, x='pickup_zone', y='count', title="Top 10 Zonas de Embarque", text='count')

    top_desembarque = dff['dropoff_zone'].value_counts().reset_index()
    top_desembarque.columns = ['dropoff_zone', 'count']
    top_desembarque = top_desembarque[top_desembarque['count'] > 0].sort_values(by='count', ascending=False).head(10)
    fig2 = px.bar(top_desembarque, x='dropoff_zone', y='count', title="Top 10 Zonas de Desembarque", text='count')

    heatmap = dff.groupby(['day_of_week', 'hour']).size().reset_index(name='count')
    fig3 = px.density_heatmap(heatmap, x='hour', y='day_of_week', z='count',
                              color_continuous_scale='Viridis', title='Volume por Dia da Semana e Hora')

    dff_2024 = dff[dff['year'] == 2024]
    top5_zonas = dff_2024['pickup_zone'].value_counts().nlargest(5).index.tolist()
    hotspots = dff[dff['pickup_zone'].isin(top5_zonas)]
    evolucao = hotspots.groupby(['season', 'pickup_zone']).size().reset_index(name='count')
    fig4 = px.line(evolucao, x='season', y='count', color='pickup_zone',
                   title='Evolução dos 5 Principais Hotspots de Embarque por Estação')

    anos = sorted(df3['year'].dropna().unique())
    estacoes = sorted(df3['season'].dropna().unique())
    servicos = sorted(df3['service_type'].dropna().unique())

    return fig1, fig2, fig3, fig4,            [{'label': str(a), 'value': a} for a in anos],            [{'label': s, 'value': s} for s in estacoes],            [{'label': s, 'value': s} for s in servicos]

# ------------------------- Callbacks TELA 4 -------------------------
@app.callback(
    Output('grafico_tarifa_mes', 'figure'),
    Output('grafico_gorjeta_mes', 'figure'),
    Output('grafico_duracao_vs_gorjeta', 'figure'),
    Output('grafico_gorjeta_servico', 'figure'),
    Output('grafico_distancia_vs_gorjeta', 'figure'),
    Input('ano', 'value'),
    Input('estacao', 'value'),
    Input('servico', 'value'),
    #prevent_initial_call=True
)

def update_tela4(ano, estacao, servico):
    dff = df4.copy()  # Cria cópia do dataframe original
    if ano:
        dff = dff[dff['year'] == ano]  # Filtra pelo ano
    if estacao:
        dff = dff[dff['season'] == estacao]  # Filtra pela estação
    if servico:
        dff = dff[dff['fk_service_type'] == servico]  # Filtra pelo tipo de serviço

    dff['trip_duration'] = (dff['dropoff_datetime'] - dff['pickup_datetime']).dt.total_seconds() / 60

    # Gráfico 1: Tarifa média por mês
    fig_tarifa = px.line(
        dff.groupby('month')['fare_amount'].mean().reset_index(),
        x='month', y='fare_amount', title='Tarifa Média por Mês'
    )

    # Gráfico 2: Gorjeta média por mês
    fig_gorjeta = px.line(
        dff.groupby('month')['tip_amount'].mean().reset_index(),
        x='month', y='tip_amount', title='Gorjeta Média por Mês'
    )

    # Gráfico 3: Relação entre duração da corrida e gorjeta
    fig_dur_gorjeta = px.scatter(
        dff, x='trip_duration', y='tip_amount',
        trendline='ols', title='Duração da Corrida vs Gorjeta'
    )

    # Gráfico 4: Distribuição de gorjetas por tipo de serviço
    fig_box_servico = px.box(
        dff, x='fk_service_type', y='tip_amount',
        title='Gorjeta por Tipo de Serviço'
    )

    # Gráfico 5: Relação entre distância percorrida e gorjeta
    fig_distancia = px.scatter(
        dff, x='trip_distance', y='tip_amount',
        trendline='ols', title='Distância da Corrida vs Gorjeta'
    )

    # Retorna os 5 gráficos atualizados
    return fig_tarifa, fig_gorjeta, fig_dur_gorjeta, fig_box_servico, fig_distancia

# ------------------------- Abrir Navegador -------------------------
def abrir_navegador():
    webbrowser.open_new("http://127.0.0.1:8050")

if __name__ == '__main__':
    threading.Timer(1.0, abrir_navegador).start()
    app.run(debug=True, port=8050)


