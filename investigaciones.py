import pandas as pd
import os
import teradata
import requests
import progressbar
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from df2gspread import df2gspread as d2g
from open_sheet import open_sheet
import json
from googleapiclient.discovery import build
from apiclient.http import MediaFileUpload
import getpass
import time

import re
import tempfile
from nltk.corpus import stopwords
import spacy
import string
from collections import Counter
from nltk.stem import WordNetLemmatizer
import collections
from wordcloud import WordCloud
import nltk
from nltk import FreqDist

formato = '%d/%m'
os.system('clear')

def query_new(sites, dominio, session):
    df_productizado = pd.read_sql_query("""SELECT top 2000 A.ite_catalog_product_id_str AS CATALOG_PRODUCT_ID, SUM(A.BID_BASE_CURRENT_PRICE * A.BID_QUANTITY_OK) AS GMV_PROD, A.ite_item_id
        FROM WHOWNER.BT_BIDS A
        WHERE substring(A.dom_domain_id,5) = '"""+dominio+"""'
        AND A.sit_site_id in ('"""+sites+"""')
        AND A.PHOTO_ID = 'TODATE'
        AND A.ITE_GMV_FLAG = 1
        AND A.ITE_TIPO_PROD = 'N'
        AND A.MKT_MARKETPLACE_ID = 'TM'
        AND A.ITE_CATALOG_LISTING = 0
        AND A.TIM_DAY_WINNING_DATE BETWEEN ADD_MONTHS(DATE,-1) AND DATE
        AND A.ite_item_id IS NOT NULL
        GROUP BY 1,3
        ORDER BY 2 desc;""",session)
    df_productizado['ITE_ITEM_ID'] = df_productizado['ITE_ITEM_ID'].astype(int)
    
    lisProd = []
    lisItem = []
    itemUnique = pd.DataFrame()
    count = 0
    for i in range(len(df_productizado)):
        if not ((df_productizado.loc[i,'CATALOG_PRODUCT_ID'] in lisProd) and (df_productizado.loc[i,'ITE_ITEM_ID'] in lisItem)):
            if count <= 29:
                itemUnique.loc[i, 'ITEM_ID'] = sites+str(df_productizado.loc[i,'ITE_ITEM_ID'])
                itemUnique.loc[i, 'PROD_ID'] = df_productizado.loc[i,'CATALOG_PRODUCT_ID']
                itemUnique.loc[i, 'GMV'] = df_productizado.loc[i,'GMV_PROD']
                if df_productizado.loc[i,'CATALOG_PRODUCT_ID'] != None: 
                    lisProd.append(df_productizado.loc[i,'CATALOG_PRODUCT_ID'])
                lisItem.append(df_productizado.loc[i,'ITE_ITEM_ID'])
                count = count + 1
            
    return itemUnique

def get_item(it,desc):
    s = requests.Session()
    if desc == False:
        url = 'https://internal-api.mercadolibre.com/items/'+str(it)
        headers = { 'Content-Type': 'application/json','X-Caller-Scopes': 'admin'}
        response = s.get(url, headers=headers)
    else:
        url = 'https://internal-api.mercadolibre.com/items/'+str(it)+'/description'
        headers = { 'Content-Type': 'application/json','X-Caller-Scopes': 'admin'}
        response = s.get(url, headers=headers)
    s.close()
    return response

def get_decorations(prod):
    s = requests.Session()
    url = 'https://internal-api.mercadolibre.com/internal/pdp/decorations/decorations/'+str(prod)
    headers = { "Content-Type": "application/json"}
    response = s.get(url, headers=headers)
    s.close()
    return response

def get_category(cate):
    s = requests.Session()
    url = 'https://internal-api.mercadolibre.com/categories/'+str(cate)
    headers = { 'Content-Type': 'application/json'}
    cat = s.get(url, headers=headers)
    cat = cat.json()
    s.close()
    return cat

def get_completitudItem(compItem):
    s = requests.Session()
    url = 'https://internal-api.mercadolibre.com/catalog_quality/status?item_id='+str(compItem)+'&incomplete_attributes=true&v=&v=3'
    headers = { 'Content-Type': 'application/json'}
    response = s.get(url, headers=headers)
    s.close()
    return response

def get_catalogProducts(prod):
    s = requests.Session()
    url = 'https://internal-api.mercadolibre.com/catalog_products/'+str(prod)
    headers = { 'Content-Type': 'application/json'}
    response = s.get(url, headers=headers)
    s.close()
    return response

def get_catalogDomains(dom, band):
    s = requests.Session()
    if band == True:            
        url = 'https://internal-api.mercadolibre.com/catalog_domains/'+str(dom)+'/products'
        headers = { "Content-Type": "application/json"}
        response = s.get(url, headers=headers)

    else:    
        url = 'https://internal-api.mercadolibre.com/catalog_domains/'+str(dom)
        headers = { 'Content-Type': 'application/json'}
        response = s.get(url, headers=headers)
        s.close()
        
    return response

def get_questions(ids):
    s = requests.Session()
    url = 'https://internal-api.mercadolibre.com/questions/search?item='+str(ids)+'&sort_fields=date_created&sort_types=DESC'
    headers = { 'Content-Type': 'application/json', 'X-Caller-Scopes': 'admin' }
    response = s.get(url, headers=headers)
    s.close()
    return response

def get_product(pro):
    s = requests.Session()
    url = "https://internal-api.mercadolibre.com/products/"+str(pro)
    headers = { "Content-Type": "application/json", "X-Caller-Scopes": "admin"}
    y = s.get(url, headers=headers)
    s.close()
    return y

def teradata_session(username_teradata,password_teradata):
    print('\nValidando datos..')
    udaExec = teradata.UdaExec(appName='MyApp', version='1.0', logConsole=False)
    session = udaExec.connect(method='odbc', 
                              system='teradata.melicloud.com', 
                              username= ""+username_teradata+"", 
                              password=""+password_teradata+"", 
                              authentication='LDAP',
                              USEREGIONALSETTINGS='N',
                              driver='Teradata Database ODBC Driver 17.00',
                              charset='UTF8')
    print('\nDatos Correctos')
    return session

def preguntas(questionsItem):
    listado = []
    for i in progressbar.progressbar(range(len(questionsItem))):
        questions = get_questions(questionsItem.loc[i,'ITEM_ID'])
        if questions.status_code in range(200,300):
            questions = questions.json()
            if questions.get('total')> 0:
                for q in questions.get('questions'):
                    if q.get('answer'):
                        answer = q.get('answer').get('text')
                    else:
                        answer = None

                    id_site = questionsItem.loc[i,'ITEM_ID'][:3]

                    listado.append({'item':q['item_id'],
                                    'pregunta':q['text'],
                                    'respuesta':answer,
                                    'site': id_site})

    quest = pd.DataFrame(listado)
    
    questions_ar = quest[quest['site'] == 'MLA']
    questions_mx = quest[quest['site'] == 'MLM']
    questions_pt = quest[quest['site'] == 'MLB']
    questions_cl = quest[quest['site'] == 'MLC']
    questions_co = quest[quest['site'] == 'MCO']
    questions_ur = quest[quest['site'] == 'MLU']

    return questions_ar, questions_mx , questions_pt, questions_cl, questions_co, questions_ur

def attributes_domain(dominio):
    site = 'MLA'
    attributes = get_catalogDomains(site+"-"+dominio, False).json().get('attributes')
    data = []
    for i in range(len(attributes)):
        data.append({
            'Code':attributes[i].get('id'),
            'Name':attributes[i].get('name'),
            'Hierarchy':attributes[i].get('tags').get('hierarchy'),
            'Relevance':attributes[i].get('tags').get('relevance')
        })
        
    data.insert(len(data)+1,{'Code': '',
    'Name':'',
    'Hierarchy':'' ,
    'Relevance':''})
    
    data.insert(len(data)+1,{'Code': 'Estado del acuerdo consolidado (GENERAL):',
    'Name':'=SI(CONTAR.SI(Q:Q;"Abierto")>0;"FALTAN VER ATRIBUTOS";"ACUERDO CERRADO")',
    'Hierarchy':'' ,
    'Relevance':''})
    
    df_attr = pd.DataFrame(data)
    df_attr = df_attr[['Code','Name','Hierarchy','Relevance']]
    
    df_attr.loc[0,'Instancia/escenario'] = ''
    df_attr.loc[0,'Solicitante'] = ''
    df_attr.loc[0,'Acción'] = ''
    df_attr.loc[0,'Prioridad PDP'] = ''
    df_attr.loc[0,'Site/s'] = ''
    df_attr.loc[0,'Explicación'] = ''
    df_attr.loc[0,'Tipo de dato sugerido'] = ''
    df_attr.loc[0,'Ejemplo del valor del atributo'] = ''
    df_attr.loc[0,'Relevancia sugerida'] = ''
    df_attr.loc[0,'Hierarchy sugerida'] = ''
    df_attr.loc[0,'Fuentes (3)'] = ''
    df_attr.loc[0,'Respuesta a la sugerencia'] = ''
    df_attr.loc[0,'Comentarios'] = ''
    df_attr.loc[0,'Estado del acuerdo'] = ''
    df_attr.loc[0,'Resolución estructura'] = ''
    df_attr.loc[0,'Fecha de cierre del acuerdo'] = ''
    df_attr.loc[0,'Link al ticket de Jira'] = ''
    df_attr.loc[0,'Fecha del pedido de modificación'] = ''

    
    df_attr = df_attr[['Code','Name','Hierarchy','Relevance','Instancia/escenario','Solicitante','Acción','Prioridad PDP','Site/s',
                      'Explicación','Tipo de dato sugerido','Ejemplo del valor del atributo','Relevancia sugerida',
                      'Hierarchy sugerida','Fuentes (3)','Fecha del pedido de modificación',
                       'Respuesta a la sugerencia','Comentarios','Estado del acuerdo','Resolución estructura',
                      'Fecha de cierre del acuerdo','Link al ticket de Jira']]

    return df_attr.fillna('')

def analisisOferta(df_it):
    print('\nProcesando el Análisis de la Oferta')
    if not df_it.empty:
        list_data = []
        for i in progressbar.progressbar(range(len(df_it))):
            df_it.loc[i, 'Ítems'] = df_it.loc[i,'ITEM_ID']
            df_it.loc[i, 'Sites'] = df_it.loc[i, 'ITEM_ID'][:3]
            df_it.loc[i, 'GMV'] = df_it.loc[i,'GMV']
            item = get_item(df_it.loc[i,'ITEM_ID'], False).json()
            pics = item.get('pictures') if item.get('pictures') != None else '0'
            attrs = item.get('attributes')

            df_it.loc[i, 'ID Categoria'] = item.get('category_id')
            df_it.loc[i, 'Name Category'] = get_category(item.get('category_id')).get('name')
            df_it.loc[i, 'URL'] = item.get('permalink')
            df_it.loc[i, 'Precio'] = str(item.get('price'))

            description = get_item(df_it.loc[i,'ITEM_ID'], True).json()
            if description != []:
                df_it.loc[i, 'Descripcion'] = description.get('plain_text')
            else:
                df_it.loc[i, 'Descripcion'] = '[]'

            attr = get_completitudItem(item.get('id'))
            if attr.status_code in range(200,300):
                attr = attr.json()
                df_it.loc[i, 'Ficha Técnica'] = 'Complete: '+str(attr.get('adoption_status').get('all').get('complete'))+"\n"+'missing_attributes: '+ str(attr.get('adoption_status').get('all').get('missing_attributes'))+"\n"+'quality_level: '+str(attr.get('adoption_status').get('quality_level'))
                df_it.loc[i, 'FT Completo'] =  attr.get('adoption_status').get('all').get('complete')
            else:
                df_it.loc[i, 'Ficha Técnica'] = 'No Aplica'
                df_it.loc[i, 'FT Completo'] =  attr.json().get('message') 

            df_it.loc[i, 'Title'] = item.get('title')
            df_it.loc[i, 'condition'] = item.get('condition')
            df_it.loc[i, 'domain_id'] = item.get('domain_id')
            df_it.loc[i, 'catalog_product_id'] = item.get('catalog_product_id')
            products = item.get('catalog_product_id')
            if products != None:
                df_it.loc[i, 'Nombre producto'] = get_catalogProducts(products).json().get('name') if get_catalogProducts(products).status_code in range(200,300) else 'Eliminado'
            else:
                df_it.loc[i, 'Nombre producto'] = ''
            df_it.loc[i, 'Kit Productizable'] = ''
            df_it.loc[i, 'Kit no Productizable'] = ''
            df_it.loc[i, 'Artesanía'] = ''
            df_it.loc[i, 'Categorías OK'] = ''
            df_it.loc[i, 'Matcheo OK'] = ''
            df_it.loc[i, 'Pack SI/NO'] = '' 
            df_it.loc[i, 'Ítem fraudulento'] = ''
            df_it.loc[i, 'Genérico SI/NO'] = ''
            df_it.loc[i, 'Ítem con datos contradictorios'] = ''
            df_it.loc[i, 'Corresponde a OD'] = ''
            df_it.loc[i, 'Ítem productizable'] = ''

            for j in range(len(attrs)):
                if attrs[j].get('id') == 'BRAND':
                    df_it.loc[i, 'BRAND'] = attrs[j].get('value_name') 
                if attrs[j].get('id') == 'MODEL':
                    df_it.loc[i, 'MODEL'] = attrs[j].get('value_name')
                if attrs[j].get('id') == 'LINE':
                    df_it.loc[i, 'LINE'] = attrs[j].get('value_name')

            count = 0
            if pics != '0':
                if len(pics) > 0:
                    for pic in pics:
                        if count < 3:
                            df_it.loc[i, 'pic'+str(count)] = pic.get('url')
                            df_it.loc[i, 'Image'+str(count)] = ''
                            count += 1
            else:
                df_it.loc[i, 'pic'+str(count)] = 'Vacio'
        df_variations = pd.DataFrame(list_data)

        if not df_variations.empty:
            df_total = df_it.merge(df_variations, on ='Ítems', how='outer')
        else:
            df_total = df_it
    else:
        print('--No tiene Ítems que generen GMV--')

    if not df_variations.empty:
        if 'LINE' in df_total:
            if 'MODEL' in df_total:
                df = df_total[['Ítems','Sites','Title', 'BRAND','MODEL', 'LINE' , 'pic0','Image0','pic1','Image1','pic2',
                         'Image2','URL','Pack SI/NO','Kit Productizable','Kit no Productizable','Artesanía', 'Genérico SI/NO','Ítem fraudulento',
                        'Ítem con datos contradictorios','Corresponde a OD','Ítem productizable','domain_id','FT Completo',
                         'condition','catalog_product_id','Precio','Descripcion','Ficha Técnica',
                         'GMV']]

            else:
                df = df_total[['Ítems','Sites','Title', 'BRAND','LINE' , 'pic0','Image0','pic1','Image1','pic2',
                         'Image2','URL','Pack SI/NO','Kit Productizable','Kit no Productizable','Artesanía', 'Genérico SI/NO','Ítem fraudulento',
                        'Ítem con datos contradictorios','Corresponde a OD','Ítem productizable','domain_id','FT Completo',
                         'condition','catalog_product_id','Precio','Descripcion','Ficha Técnica',
                         'GMV']]
        elif 'MODEL' in df_total:
            df = df_total[['Ítems','Sites','Title', 'BRAND','MODEL', 'pic0','Image0','pic1','Image1','pic2',
                         'Image2','URL','Pack SI/NO','Kit Productizable','Kit no Productizable','Artesanía', 'Genérico SI/NO','Ítem fraudulento',
                        'Ítem con datos contradictorios','Corresponde a OD','Ítem productizable', 'domain_id','FT Completo',
                         'condition','catalog_product_id','Precio','Descripcion','Ficha Técnica',
                         'GMV']]
        else:
            df = df_total[['Ítems','Sites','Title', 'BRAND', 'pic0','Image0','pic1','Image1','pic2',
                         'Image2','URL','Pack SI/NO','Kit Productizable','Kit no Productizable','Artesanía', 'Genérico SI/NO','Ítem fraudulento',
                        'Ítem con datos contradictorios','Corresponde a OD','Ítem productizable','Name Category','domain_id','FT Completo',
                         'condition','catalog_product_id','Precio','Descripcion','Ficha Técnica',
                         'GMV']]

    else:
        if 'LINE' in df_total:
            if 'MODEL' in df_total:
                df = df_total[['Ítems','Sites','Title', 'BRAND','MODEL', 'LINE' , 'pic0','Image0','pic1','Image1','pic2',
                         'Image2','URL','Pack SI/NO','Kit Productizable','Kit no Productizable','Artesanía', 'Genérico SI/NO','Ítem fraudulento',
                        'Ítem con datos contradictorios','Corresponde a OD','Ítem productizable','domain_id','FT Completo',
                         'condition','catalog_product_id','Precio','Descripcion','Ficha Técnica',
                         'GMV']]
            else:
                df = df_total[['Ítems','Sites','Title', 'BRAND', 'LINE' , 'pic0','Image0','pic1','Image1','pic2',
                         'Image2','URL','Pack SI/NO','Kit Productizable','Kit no Productizable','Artesanía', 'Genérico SI/NO','Ítem fraudulento',
                        'Ítem con datos contradictorios','Corresponde a OD','Ítem productizable','domain_id','FT Completo',
                         'condition','catalog_product_id','Precio','Descripcion','Ficha Técnica',
                         'GMV']]
        elif 'MODEL' in df_total:
            df = df_total[['Ítems','Sites','Title', 'BRAND','MODEL' , 'pic0','Image0','pic1','Image1','pic2',
                         'Image2','URL','Pack SI/NO','Kit Productizable','Kit no Productizable','Artesanía', 'Genérico SI/NO','Ítem fraudulento',
                        'Ítem con datos contradictorios','Corresponde a OD','Ítem productizable','domain_id','FT Completo',
                         'condition','catalog_product_id','Precio','Descripcion','Ficha Técnica',
                         'GMV']]
        else:
            df = df_total[['Ítems','Sites','Title', 'BRAND', 'pic0','Image0','pic1','Image1','pic2',
                         'Image2','URL','Pack SI/NO','Kit Productizable','Kit no Productizable','Artesanía', 'Genérico SI/NO','Ítem fraudulento',
                        'Ítem con datos contradictorios','Corresponde a OD','Ítem productizable','domain_id','FT Completo',
                         'condition','catalog_product_id','Precio','Descripcion','Ficha Técnica',
                         'GMV']]

    df_brands = pd.DataFrame(df_total.groupby(by=['BRAND','Sites']).count()['GMV']).reset_index()
    df_brands.columns = ['BRAND', 'Sites', 'Cant']
    df_brands= df_brands.sort_values(by='Cant', ascending=False)

    return df, df_brands

def wordcloud(questions,idioma):
    if not questions.empty:
        questions = questions.reset_index(drop=True)
        value=True
        idioma = idioma
        #1. Genero el corpus (la lista de listas de texto) 
        corpus = []
        for c in range(len(questions)):
            preg = questions.loc[c,'pregunta']
            corpus.append(nltk.word_tokenize(preg))

        flatten = [w for l in corpus for w in l]
        
        # 2. Preparo el texto para tokenizarlo
        text = ""
        for item in flatten:
            text += item + str(' ')

        #3. Tokenizo por palabras con el pattern de a continuacion (pattern: forma en la que nltk identifica palabras en el corpus)
        pattern = r'''(?x)                  # Flag para iniciar el modo verbose
                      (?:[A-Z]\.)+            # Hace match con abreviaciones como U.S.A.
                      | \w+(?:-\w+)*         # Hace match con palabras que pueden tener un guión interno
                      | \$?\d+(?:\.\d+)?%?  # Hace match con dinero o porcentajes como $15.5 o 100%
                      | \.\.\.              # Hace match con puntos suspensivos
                      | [][.,;"'?():-_`]    # Hace match con signos de puntuación
        '''

        word_token = nltk.regexp_tokenize(text, pattern= pattern)
        
        #4. Llamo las stopwords de nltk
        stop_word_es = stopwords.words('spanish')
        stop_word_pt = stopwords.words('portuguese')

        #5. Defino las palabras que no me agregan valor tanto en español como en portuges 
        bag_of_words_es = ['compra','compro','comprar','comrpado','compre', 'Hola', 'hola', 'Chau', 'chau','Como'
                      'Gracias','gracias','Muchas', 'muchas','gracia','salir','Salir','tenes', 'Tenes',
                      'GraciasHola','salir','pasar','tienes','Tienes','Tienen','Tiene','tienen','tiene','cuanto', 'Cuanto','llegar','tenés',
                      'stock?Hola','graciasHola','tener','....','sale','saldría','retirar','envío','Envio','envio','Envios','envios','Envío','envío',
                      'Envíos','envíos','días','Días','Día','día','envió','precio','tardes','factura','quiero','quería','quisiera','pague','pago',
                      'Saludos','saludos','pagar','pagamos','pagando','agregar','Stock','stock','podes','traer','mañana','Mañanas',
                      'saludos','publicacion', 'Pará', 'tenes', 'tenés','Buen','buen','buenas','Buenas','buena','Buena','noches','Noches',
                     'este','Cómo','cómo','Dónde','dónde','Cuál','cuál','bien','Buenos','Bueno','bueno','buenos','Dia','dia','Dias','dias','https','http',
                     'tarjeta','Tarjeta', 'efectivo', 'precio', 'envío', 'mercado', 'cuotas', 'factura', 'financiación',
                      'visa', 'dolar', 'transferencia', 'banco', 'tarjeta', 'pagar', 'usado','Gratis','gratis','Gracias','gracias','Pueden','pueden','Puede','puede','Puedo','puedo',
                      'Viene','viene','vienen','Vienen','Preguntas','preguntas','Entrega','entrega','Entregar','entregar','Hace','hace','Hacer',
                      'hacer','Hacen','hacen','Trae','trae','Traer','traer','venden','Venden','dice','Dice','dicen','Dicen']

        bag_of_words_pt = ['comprar', 'comprou', 'Olá', 'olá', 'Tchau', 'tchau', 'Como','Obrigado', 'obrigado', 'Muitos', 'muitos', 'graça',
                       'saia', 'você tem','Você tem','Obrigado Olá', 'sair', 'Sair', 'passar', 'Ter', 'quanto', 'quanto', 'chegar', 
                      'estoque? Olá', 'obrigadoOlá', 'ter', '....', 'retirar', 'Envio', 'envio', 'Dias', 'dias','Dia', 'dia'
                      'enviado', 'preço', 'tardes', 'fatura', 'Desejo', 'desejo', 'gostaria de', 'pagar','Pagar', 'pagamento', 'Saudações', 'saudações',
                       'nós pagamos', 'pagando', 'adicionar', 'Estoque', 'estoque', 'pode', 'trazer', 'Trazer', 'amanhã', 'Manhãs',
                      'Saudações', 'Publicação', 'Parar', 'bom', 'Bom', 'Noite', 'Noites', 'isto', 'como', 'onde', 'onde', 'Qual', 'qual', ' https ',' http ',
                     'cartão', 'cartão', 'dinheiro', 'preço', 'frete', 'mercado', 'parcelamento', 'fatura', 'financiamento',
                      'visa', 'dólar', 'transferir', 'banco', 'cartão', 'usado', 'obrigado', 'obrigado', 'pode', 
                      'Vem', 'vem', 'Eles vêm', 'Perguntas', 'perguntas', 'Entrega', 'entrega', 'entrega', 'Faz', 'faz']


        #6. Filtro las palabras (tokens)
        if idioma =='es':
            word_token_test = [palabra for palabra in word_token if len(palabra)>3 and palabra not in bag_of_words_es and palabra not in stop_word_es]
        else:
            word_token_test = [palabra for palabra in word_token if len(palabra)>3 and palabra not in bag_of_words_pt and palabra not in stop_word_pt]
        
        #7. Defino y ordeno los 40 token con mayor frecuencia dentro del corpus (me queda un array)
        dist = FreqDist(word_token_test)
        importantes = pd.DataFrame(dist.most_common(40), columns=['word','freq'])

        #8. Transformo el array en un diccionario para graficarlo
        count = 0
        for i in range(len(importantes)):
            if count == 0:
                important = {importantes.iloc[i]['word']: int(importantes.iloc[i]['freq'])}

            else:
                important1 = {importantes.iloc[i]['word']: int(importantes.iloc[i]['freq'])}

                important.update(important1)

            count = count + 1

        #9. Genero la wordcloud
        wc = WordCloud(width=430, height=250,background_color="white").generate_from_frequencies(important)
    else:
        value = False
        wc = print('No se encontraron pregunas en la investigación')

    return wc, value

def creden():
    scope_gdoc = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    cre = ServiceAccountCredentials.from_json_keyfile_name('', scope_gdoc)
    gc = gspread.authorize(cre)

    return cre, gc, open_sheet, d2g


def load_wc(dominio, pregs_ar, pregs_mx , pregs_pt, pregs_cl, pregs_co, pregs_ur, flag):
    idiomas=['es','pt']
    #Cargo las credenciales
    cre = creden()[0]
    
    #Creo el servicio de Google Drive
    service = build('drive', 'v3', credentials=cre)
    
    #Ingreso a la carpeta de Google Drive
    folder_id = '1zwqLW9Meq4rmIZIEv0VFNREC5K58QlmC'
    query = f"mimeType='image/jpeg' and parents in '{folder_id}' and trashed = false"

    response = service.files().list(corpora = 'drive',
                                    driveId ='0ANkDhFpxGjvEUk9PVA',
                                    q=query,
                                    includeItemsFromAllDrives = True,
                                    supportsAllDrives= True).execute()
    
    #Guardo el nombre de los archivos que contiene para luego validar si hago un update o creo un archivo nuevo
    pics = []
    for i in range(len(response.get('files'))):
        if response.get('files')[i].get('name'):
            pics.append({'name': response.get('files')[i].get('name'),
                        'id':response.get('files')[i].get('id')})
    
    #Creo el dataframe para luego buscar el nombre
    if pics != []:
        df_pics = pd.DataFrame(pics)
        for idioma in idiomas:
            if idioma == 'es':
                if not pregs_ar.empty:
                    wc , value = wordcloud(pregs_ar,idioma)

                    if value == True:
                        plt.figure(figsize=(30,30))
                        plt.title(label= 'MLA_'+str(dominio), fontsize=40)
                        plt.imshow(wc, interpolation="bilinear")
                        plt.axis("off")
                        plt.savefig(tempfile.gettempdir()+'/'+'MLA_'+str(dominio)+'.jpg')

                    cre = creden()[0]
                    service = build('drive', 'v3', credentials=cre)

                    #Valido si existe o no la wordcloud
                    file_name = 'MLA_'+str(dominio)+'.jpg'

                    if file_name in list(df_pics.name):
                        file_metadata = {'name' : file_name}

                        media = MediaFileUpload(tempfile.gettempdir()+'/'+'MLA_'+str(dominio)+'.jpg',
                                                mimetype='image/jpeg')

                        service.files().update(fileId=list(df_pics[df_pics['name']==file_name].id)[0],
                                               media_body=media,
                                               supportsAllDrives = True).execute()
                    else:
                        folder_id = ['1zwqLW9Meq4rmIZIEv0VFNREC5K58QlmC']
                        file_metadata = {'name' : file_name,
                                        'parents':folder_id
                                         }
                        media = MediaFileUpload(tempfile.gettempdir()+'/'+'MLA_'+str(dominio)+'.jpg',
                                                mimetype='image/jpeg',
                                                resumable=True)

                        service.files().create(body=file_metadata, media_body=media, supportsAllDrives = True).execute()

                    print('WordCloud MLA cargda')
                else:
                    print('El site MLA, no cuenta con preguntas')

                if not pregs_mx.empty:

                    wc , value = wordcloud(pregs_mx,idioma)

                    if value == True:

                        plt.figure(figsize=(30,30))
                        plt.title(label= 'MLM_'+str(dominio), fontsize=40)
                        plt.imshow(wc, interpolation="bilinear")
                        plt.axis("off")
                        plt.savefig(tempfile.gettempdir()+'/'+'MLM_'+str(dominio)+'.jpg')

                    cre = creden()[0]
                    service = build('drive', 'v3', credentials=cre)

                    #Valido si existe o no la wordcloud
                    file_name = 'MLM_'+str(dominio)+'.jpg'

                    if file_name in list(df_pics.name):
                        file_metadata = {'name' : file_name}

                        media = MediaFileUpload(tempfile.gettempdir()+'/'+'MLM_'+str(dominio)+'.jpg',
                                                mimetype='image/jpeg')

                        service.files().update(fileId=list(df_pics[df_pics['name']==file_name].id)[0],
                                               media_body=media,
                                               supportsAllDrives = True).execute()
                    else:
                        folder_id = ['1zwqLW9Meq4rmIZIEv0VFNREC5K58QlmC']
                        file_metadata = {'name' : file_name,
                                        'parents':folder_id
                                         }
                        media = MediaFileUpload(tempfile.gettempdir()+'/'+'MLM_'+str(dominio)+'.jpg',
                                                mimetype='image/jpeg',
                                                resumable=True)

                        service.files().create(body=file_metadata, media_body=media, supportsAllDrives = True).execute()


                    print('WordCloud MLM cargda')
                else:
                    print('El site MLM, no cuenta con preguntas')
                
                if not pregs_cl.empty:

                    wc , value = wordcloud(pregs_cl,idioma)

                    if value == True:

                        plt.figure(figsize=(30,30))
                        plt.title(label= 'MLC_'+str(dominio), fontsize=40)
                        plt.imshow(wc, interpolation="bilinear")
                        plt.axis("off")
                        plt.savefig(tempfile.gettempdir()+'/'+'MLC_'+str(dominio)+'.jpg')

                    cre = creden()[0]
                    service = build('drive', 'v3', credentials=cre)

                    #Valido si existe o no la wordcloud
                    file_name = 'MLC_'+str(dominio)+'.jpg'

                    if file_name in list(df_pics.name):
                        file_metadata = {'name' : file_name}

                        media = MediaFileUpload(tempfile.gettempdir()+'/'+'MLC_'+str(dominio)+'.jpg',
                                                mimetype='image/jpeg')

                        service.files().update(fileId=list(df_pics[df_pics['name']==file_name].id)[0],
                                               media_body=media,
                                               supportsAllDrives = True).execute()
                    else:
                        folder_id = ['1zwqLW9Meq4rmIZIEv0VFNREC5K58QlmC']
                        file_metadata = {'name' : file_name,
                                        'parents':folder_id
                                         }
                        media = MediaFileUpload(tempfile.gettempdir()+'/'+'MLC_'+str(dominio)+'.jpg',
                                                mimetype='image/jpeg',
                                                resumable=True)

                        service.files().create(body=file_metadata, media_body=media, supportsAllDrives = True).execute()


                    print('WordCloud MLC cargda')
                else:
                    if flag ==2:
                        print('El site MLC, no cuenta con preguntas')

                if not pregs_co.empty:

                    wc , value = wordcloud(pregs_co,idioma)

                    if value == True:

                        plt.figure(figsize=(30,30))
                        plt.title(label= 'MCO_'+str(dominio), fontsize=40)
                        plt.imshow(wc, interpolation="bilinear")
                        plt.axis("off")
                        plt.savefig(tempfile.gettempdir()+'/'+'MCO_'+str(dominio)+'.jpg')

                    cre = creden()[0]
                    service = build('drive', 'v3', credentials=cre)

                    #Valido si existe o no la wordcloud
                    file_name = 'MCO_'+str(dominio)+'.jpg'

                    if file_name in list(df_pics.name):
                        file_metadata = {'name' : file_name}

                        media = MediaFileUpload(tempfile.gettempdir()+'/'+'MCO_'+str(dominio)+'.jpg',
                                                mimetype='image/jpeg')

                        service.files().update(fileId=list(df_pics[df_pics['name']==file_name].id)[0],
                                               media_body=media,
                                               supportsAllDrives = True).execute()
                    else:
                        folder_id = ['1zwqLW9Meq4rmIZIEv0VFNREC5K58QlmC']
                        file_metadata = {'name' : file_name,
                                        'parents':folder_id
                                         }
                        media = MediaFileUpload(tempfile.gettempdir()+'/'+'MCO_'+str(dominio)+'.jpg',
                                                mimetype='image/jpeg',
                                                resumable=True)

                        service.files().create(body=file_metadata, media_body=media, supportsAllDrives = True).execute()


                    print('WordCloud MCO cargda')
                else:
                    if flag ==2:
                        print('El site MCO, no cuenta con preguntas')

                if not pregs_ur.empty:

                    wc , value = wordcloud(pregs_ur,idioma)

                    if value == True:

                        plt.figure(figsize=(30,30))
                        plt.title(label= 'MLU_'+str(dominio), fontsize=40)
                        plt.imshow(wc, interpolation="bilinear")
                        plt.axis("off")
                        plt.savefig(tempfile.gettempdir()+'/'+'MLU_'+str(dominio)+'.jpg')

                    cre = creden()[0]
                    service = build('drive', 'v3', credentials=cre)

                    #Valido si existe o no la wordcloud
                    file_name = 'MLU_'+str(dominio)+'.jpg'

                    if file_name in list(df_pics.name):
                        file_metadata = {'name' : file_name}

                        media = MediaFileUpload(tempfile.gettempdir()+'/'+'MLU_'+str(dominio)+'.jpg',
                                                mimetype='image/jpeg')

                        service.files().update(fileId=list(df_pics[df_pics['name']==file_name].id)[0],
                                               media_body=media,
                                               supportsAllDrives = True).execute()
                    else:
                        folder_id = ['1zwqLW9Meq4rmIZIEv0VFNREC5K58QlmC']
                        file_metadata = {'name' : file_name,
                                        'parents':folder_id
                                         }
                        media = MediaFileUpload(tempfile.gettempdir()+'/'+'MLU_'+str(dominio)+'.jpg',
                                                mimetype='image/jpeg',
                                                resumable=True)

                        service.files().create(body=file_metadata, media_body=media, supportsAllDrives = True).execute()


                    print('WordCloud MLU cargda')
                else:
                    if flag ==2:
                        print('El site MLU, no cuenta con preguntas')


            else:
                idioma = 'pt'
                if not pregs_pt.empty:

                    wc , value = wordcloud(pregs_pt,idioma)

                    if value == True:

                        plt.figure(figsize=(30,30))
                        plt.title(label= 'MLB_'+str(dominio), fontsize=40)
                        plt.imshow(wc, interpolation="bilinear")
                        plt.axis("off")
                        plt.savefig(tempfile.gettempdir()+'/'+'MLB_'+str(dominio)+'.jpg')


                    cre = creden()[0]
                    service = build('drive', 'v3', credentials=cre)

                    file_name = 'MLB_'+str(dominio)+'.jpg'

                    if file_name in list(df_pics.name):
                        file_metadata = {'name' : file_name}

                        media = MediaFileUpload(tempfile.gettempdir()+'/'+'MLB_'+str(dominio)+'.jpg',
                                                mimetype='image/jpeg')

                        service.files().update(fileId=list(df_pics[df_pics['name']==file_name].id)[0],
                                               media_body=media,
                                               supportsAllDrives = True).execute()
                    else:
                        folder_id = ['1zwqLW9Meq4rmIZIEv0VFNREC5K58QlmC']
                        file_metadata = {'name' : file_name,
                                        'parents':folder_id
                                         }
                        media = MediaFileUpload(tempfile.gettempdir()+'/'+'MLB_'+str(dominio)+'.jpg',
                                                mimetype='image/jpeg',
                                                resumable=True)

                        service.files().create(body=file_metadata, media_body=media, supportsAllDrives = True).execute()

                    print('WordCloud MLB cargda')
                else:
                    print('El site MLB, no cuenta con preguntas')
                    
    else:
        for idioma in idiomas:
            if idioma == 'es':
                if not pregs_ar.empty:
                    wc , value = wordcloud(pregs_ar,idioma)
                    if value == True:
                        plt.figure(figsize=(30,30))
                        plt.title(label= 'MLA_'+str(dominio), fontsize=40)
                        plt.imshow(wc, interpolation="bilinear")
                        plt.axis("off")
                        plt.savefig(tempfile.gettempdir()+'/'+'MLA_'+str(dominio)+'.jpg')

                    cre = creden()[0]
                    service = build('drive', 'v3', credentials=cre)

                    #Valido si existe o no la wordcloud
                    file_name = 'MLA_'+str(dominio)+'.jpg'

                    folder_id = ['1zwqLW9Meq4rmIZIEv0VFNREC5K58QlmC']
                    file_metadata = {'name' : file_name,
                                    'parents':folder_id
                                     }
                    media = MediaFileUpload(tempfile.gettempdir()+'/'+'MLA_'+str(dominio)+'.jpg',
                                            mimetype='image/jpeg',
                                            resumable=True)

                    service.files().create(body=file_metadata, media_body=media, supportsAllDrives = True).execute()

                    print('WordCloud MLA cargda')
                else:
                    print('El site MLA, no cuenta con preguntas')

                if not pregs_mx.empty:

                    wc , value = wordcloud(pregs_mx,idioma)

                    if value == True:

                        plt.figure(figsize=(30,30))
                        plt.title(label= 'MLM_'+str(dominio), fontsize=40)
                        plt.imshow(wc, interpolation="bilinear")
                        plt.axis("off")
                        plt.savefig(tempfile.gettempdir()+'/'+'MLM_'+str(dominio)+'.jpg')

                    cre = creden()[0]
                    service = build('drive', 'v3', credentials=cre)

                    #Valido si existe o no la wordcloud
                    file_name = 'MLM_'+str(dominio)+'.jpg'

                    folder_id = ['1zwqLW9Meq4rmIZIEv0VFNREC5K58QlmC']
                    file_metadata = {'name' : file_name,
                                    'parents':folder_id
                                     }
                    media = MediaFileUpload(tempfile.gettempdir()+'/'+'MLM_'+str(dominio)+'.jpg',
                                            mimetype='image/jpeg',
                                            resumable=True)

                    service.files().create(body=file_metadata, media_body=media, supportsAllDrives = True).execute()


                    print('WordCloud MLM cargda')
                else:
                    print('El site MLM, no cuenta con preguntas')

                if not pregs_cl.empty:

                    wc , value = wordcloud(pregs_cl,idioma)

                    if value == True:

                        plt.figure(figsize=(30,30))
                        plt.title(label= 'MLC_'+str(dominio), fontsize=40)
                        plt.imshow(wc, interpolation="bilinear")
                        plt.axis("off")
                        plt.savefig(tempfile.gettempdir()+'/'+'MLC_'+str(dominio)+'.jpg')

                    cre = creden()[0]
                    service = build('drive', 'v3', credentials=cre)

                    #Valido si existe o no la wordcloud
                    file_name = 'MLC_'+str(dominio)+'.jpg'

                    folder_id = ['1zwqLW9Meq4rmIZIEv0VFNREC5K58QlmC']
                    file_metadata = {'name' : file_name,
                                    'parents':folder_id
                                     }
                    media = MediaFileUpload(tempfile.gettempdir()+'/'+'MLC_'+str(dominio)+'.jpg',
                                            mimetype='image/jpeg',
                                            resumable=True)

                    service.files().create(body=file_metadata, media_body=media, supportsAllDrives = True).execute()


                    print('WordCloud MLC cargda')
                else:
                    print('El site MLC, no cuenta con preguntas')
                
                if not pregs_co.empty:

                    wc , value = wordcloud(pregs_co,idioma)

                    if value == True:

                        plt.figure(figsize=(30,30))
                        plt.title(label= 'MCO_'+str(dominio), fontsize=40)
                        plt.imshow(wc, interpolation="bilinear")
                        plt.axis("off")
                        plt.savefig(tempfile.gettempdir()+'/'+'MCO_'+str(dominio)+'.jpg')

                    cre = creden()[0]
                    service = build('drive', 'v3', credentials=cre)

                    #Valido si existe o no la wordcloud
                    file_name = 'MCO_'+str(dominio)+'.jpg'

                    folder_id = ['1zwqLW9Meq4rmIZIEv0VFNREC5K58QlmC']
                    file_metadata = {'name' : file_name,
                                    'parents':folder_id
                                     }
                    media = MediaFileUpload(tempfile.gettempdir()+'/'+'MCO_'+str(dominio)+'.jpg',
                                            mimetype='image/jpeg',
                                            resumable=True)

                    service.files().create(body=file_metadata, media_body=media, supportsAllDrives = True).execute()


                    print('WordCloud MCO cargda')
                else:
                    print('El site MCO, no cuenta con preguntas')

                if not pregs_ur.empty:

                    wc , value = wordcloud(pregs_ur,idioma)

                    if value == True:

                        plt.figure(figsize=(30,30))
                        plt.title(label= 'MLU_'+str(dominio), fontsize=40)
                        plt.imshow(wc, interpolation="bilinear")
                        plt.axis("off")
                        plt.savefig(tempfile.gettempdir()+'/'+'MLU_'+str(dominio)+'.jpg')

                    cre = creden()[0]
                    service = build('drive', 'v3', credentials=cre)

                    #Valido si existe o no la wordcloud
                    file_name = 'MLU_'+str(dominio)+'.jpg'

                    folder_id = ['1zwqLW9Meq4rmIZIEv0VFNREC5K58QlmC']
                    file_metadata = {'name' : file_name,
                                    'parents':folder_id
                                     }
                    media = MediaFileUpload(tempfile.gettempdir()+'/'+'MLU_'+str(dominio)+'.jpg',
                                            mimetype='image/jpeg',
                                            resumable=True)

                    service.files().create(body=file_metadata, media_body=media, supportsAllDrives = True).execute()


                    print('WordCloud MLU cargda')
                else:
                    print('El site MLU, no cuenta con preguntas')
        
            else:
                idioma = 'pt'
                if not pregs_pt.empty:

                    wc , value = wordcloud(pregs_pt,idioma)

                    if value == True:

                        plt.figure(figsize=(30,30))
                        plt.title(label= 'MLB_'+str(dominio), fontsize=40)
                        plt.imshow(wc, interpolation="bilinear")
                        plt.axis("off")
                        plt.savefig(tempfile.gettempdir()+'/'+'MLB_'+str(dominio)+'.jpg')


                    cre = creden()[0]
                    service = build('drive', 'v3', credentials=cre)

                    file_name = 'MLB_'+str(dominio)+'.jpg'

                    folder_id = ['1zwqLW9Meq4rmIZIEv0VFNREC5K58QlmC']
                    file_metadata = {'name' : file_name,
                                    'parents':folder_id
                                     }
                    media = MediaFileUpload(tempfile.gettempdir()+'/'+'MLB_'+str(dominio)+'.jpg',
                                            mimetype='image/jpeg',
                                            resumable=True)

                    service.files().create(body=file_metadata, media_body=media, supportsAllDrives = True).execute()

                    print('WordCloud MLB cargda')
                else:
                    print('El site MLB, no cuenta con preguntas')

def metafa(df_existe, dom, procesados):
    print("\nBuscando los datos de cada Familia")
    ##Cargamos todos los datos de los hijos, armamos una matrices
    if not procesados:
        metafamilia_df = pd.DataFrame()
        m = 0
    else:
        metafamilia_df = df_existe.values.tolist()

    for k in progressbar.progressbar(range(len(df_existe))):
        if df_existe.loc[k,'children_ids'] != []:
            childs = df_existe.loc[k,'children_ids']
            for p in range(len(childs)):

                childs_mo = get_catalogProducts(childs[p])
                if childs[p] not in  procesados:
                    if childs_mo.status_code in range(200,300):
                        childs_mo = childs_mo.json()
                        name = childs_mo.get('name')
                        padre = childs_mo.get('parent_id')

                        if childs_mo.get('id').find("MLA") == 0:
                            metafamilia_df.loc[m,'Id'] = padre.strip('MLA')
                            metafamilia_df.loc[m,'site'] = 'MLA'
                            metafamilia_df.loc[m,'parent_id'] = padre
                            metafamilia_df.loc[m,'children_ids'] = childs_mo.get('id')
                            metafamilia_df.loc[m,'Name'] = name
                            
                            for a in childs_mo.get('attributes'):
                                metafamilia_df.loc[m, 'attr_'+str(a['id'])] = str(a['value_name'])
                            
                        elif childs_mo.get('id').find("MLB") == 0:
                            metafamilia_df.loc[m,'Id'] = padre.strip('MLB')
                            metafamilia_df.loc[m,'site'] = 'MLB'
                            metafamilia_df.loc[m,'parent_id'] = padre
                            metafamilia_df.loc[m,'children_ids'] = childs_mo.get('id')
                            metafamilia_df.loc[m,'Name'] = name
                            
                            for a in childs_mo.get('attributes'):
                                metafamilia_df.loc[m, 'attr_'+str(a['id'])] = str(a['value_name'])

                        else:
                            metafamilia_df.loc[m,'Id'] = padre.strip('MLM')
                            metafamilia_df.loc[m,'site'] = 'MLM'
                            metafamilia_df.loc[m,'parent_id'] = padre
                            metafamilia_df.loc[m,'children_ids'] = childs_mo.get('id')
                            metafamilia_df.loc[m,'Name'] = name
                            
                            for a in childs_mo.get('attributes'):
                                metafamilia_df.loc[m, 'attr_'+str(a['id'])] = str(a['value_name'])
                                            
                        procesados.append(childs_mo.get('id'))
                        m = m + 1

                    pd.to_pickle(procesados,'./Backup/procesados_'+str (dom))
                    pd.to_pickle(metafamilia_df,'./Backup/bckp_meta_'+str (dom))
        else:
            if df_existe.loc[k,'parent_id'] not in procesados:
                if df_existe.loc[k,'parent_id'].find("MLA") == 0:                    
                    metafamilia_df.loc[m,'Id'] = df_existe.loc[k,'parent_id'].strip('MLA')
                    metafamilia_df.loc[m,'site'] = 'MLA'
                    metafamilia_df.loc[m,'parent_id'] = df_existe.loc[k,'parent_id']
                    metafamilia_df.loc[m,'children_ids'] = ''
                    metafamilia_df.loc[m,'Name'] = df_existe.loc[k,'name_parent']

                    prodParent = get_catalogProducts(df_existe.loc[k,'parent_id']).json()
                    
                    for a in prodParent.get('attributes'):
                        metafamilia_df.loc[m, 'attr_'+str(a['id'])] = str(a['value_name'])

                if df_existe.loc[k,'parent_id'].find("MLB") == 0:
                    metafamilia_df.loc[m,'Id'] = df_existe.loc[k,'parent_id'].strip('MLB')
                    metafamilia_df.loc[m,'site'] = 'MLB'
                    metafamilia_df.loc[m,'parent_id'] = df_existe.loc[k,'parent_id']
                    metafamilia_df.loc[m,'children_ids'] = ''
                    metafamilia_df.loc[m,'Name'] = df_existe.loc[k,'name_parent']

                    prodParent = get_catalogProducts(df_existe.loc[k,'parent_id']).json()
                    
                    for a in prodParent.get('attributes'):
                        metafamilia_df.loc[m, 'attr_'+str(a['id'])] = str(a['value_name'])

                if df_existe.loc[k,'parent_id'].find("MLM") == 0:
                    metafamilia_df.loc[m,'Id'] = df_existe.loc[k,'parent_id'].strip('MLM')
                    metafamilia_df.loc[m,'site'] = 'MLM'
                    metafamilia_df.loc[m,'parent_id'] = df_existe.loc[k,'parent_id']
                    metafamilia_df.loc[m,'children_ids'] = ''
                    metafamilia_df.loc[m,'Name'] = df_existe.loc[k,'name_parent']

                    prodParent = get_catalogProducts(df_existe.loc[k,'parent_id']).json()
                    
                    for a in prodParent.get('attributes'):
                        metafamilia_df.loc[m, 'attr_'+str(a['id'])] = str(a['value_name'])

                procesados.append(df_existe.loc[k,'parent_id'])
                m = m + 1
            pd.to_pickle(procesados,'./Backup/procesados_'+str (dom))
            pd.to_pickle(metafamilia_df,'./Backup/bckp_meta_'+str (dom))   

    metafamilia_df = metafamilia_df.sort_values(by='Name').fillna('').reset_index(drop=True)

    metafamilia_df = imagenProd(metafamilia_df)

    pd.to_pickle(metafamilia_df,'./Backup/bckp_meta_'+str (dom))
    return metafamilia_df

def metafaSanity(existe_df):
    print("\nBuscando los datos de cada Familia")
    metafamilia = []
    for k in progressbar.progressbar(range(len(existe_df))):
        if existe_df.loc[k,'children_ids'] != []:
            for p in range(len(existe_df.loc[k,'children_ids'])):
                get_decorations(existe_df.loc[k,'children_ids'][p])
                childs_mo = get_product(existe_df.loc[k,'children_ids'][p])
                if childs_mo.status_code in range(200,300):
                    pick = False
                    picker = []     
                    childs_mo = childs_mo.json()
                    name = childs_mo['name']
                    padre = childs_mo['parent_id']

                    lista_mf_mo = []
                    mf_not_empty_mo = 0

                    mf_childs_mo = childs_mo.get('main_features') if childs_mo.get('main_features') != None else 'null'

                    if len(mf_childs_mo) > 0 and mf_childs_mo != 'null':
                        for mf in mf_childs_mo:
                            if mf.get('text') != '':
                                lista_mf_mo = (str(lista_mf_mo) + str(mf.get('text'))+"\n").replace('[]',"")
                                mf_not_empty_mo = mf_not_empty_mo + 1
                    
                    images = childs_mo.get('pictures')
                    lista_url = []
                    if images != None:
                        for im in range(len(images)):
                            if pick == False:
                                if images[im].get('suggested_for_picker') != []:
                                    picker = images[im].get('suggested_for_picker')
                                    pick = True
                                else:
                                    picker = []
                        for im in images:
                            lista_url.append(im.get('url'))
                    else:
                        childProd = get_catalogProducts(childs_mo.get('id')).json().get('images')
                        for im in childProd:
                            lista_url.append(im['url'])

                    if childs_mo.get('id').find("MLA") == 0:
                        metafamilia.append({'id': padre.strip('MLA'),
                                            'site': 'MLA',
                                            'domain_id': childs_mo.get('domain_id'),
                                            'parent_id': padre,
                                            'children_ids': childs_mo.get('id'),
                                            'status': childs_mo.get('status'),
                                            'Name': name,
                                            'q_main_features': mf_not_empty_mo,
                                            'main_features': lista_mf_mo,
                                            'suggested_for_picker': picker, 
                                            'q_picker': len(childs_mo.get('pickers')) if childs_mo.get('pickers') != None else 'null',
                                            'q_pictures': len(lista_url) if childs_mo.get('pictures') != None else 'null',
                                            'pictures': lista_url                                                                                             
                                        })

                    elif childs_mo.get('id').find("MLB") == 0:
                        metafamilia.append({'id': padre.strip('MLB'),
                                            'site': 'MLB',
                                            'domain_id': childs_mo.get('domain_id'),
                                            'parent_id': padre,
                                            'children_ids': childs_mo.get('id'),
                                            'status': childs_mo.get('status'),
                                            'Name': name,
                                            'q_main_features': mf_not_empty_mo,
                                            'main_features': lista_mf_mo,
                                            'suggested_for_picker': picker,                                               
                                            'q_picker': len(childs_mo.get('pickers')) if childs_mo.get('pickers') != None else 'null',
                                            'q_pictures': len(lista_url) if childs_mo.get('pictures') != None else 'null',
                                            'pictures': lista_url                                                                                             
                                        })

                    else:
                        metafamilia.append({'id': padre.strip('MLM'),
                                            'site': 'MLM',
                                            'domain_id': childs_mo.get('domain_id'),
                                            'parent_id': padre,
                                            'children_ids': childs_mo.get('id'),
                                            'status': childs_mo.get('status'),
                                            'Name': name,
                                            'q_main_features': mf_not_empty_mo,
                                            'main_features': lista_mf_mo,
                                            'suggested_for_picker': picker,                                               
                                            'q_picker': len(childs_mo.get('pickers')) if childs_mo.get('pickers') != None else 'null',
                                            'q_pictures': len(lista_url) if childs_mo.get('pictures') != None else 'null',
                                            'pictures': lista_url                                                                                             
                                        })
        else:
            get_decorations(existe_df.loc[k,'parent_id'])
            if existe_df.loc[k,'parent_id'].find("MLA") == 0:
                mla_pa = get_product(existe_df.loc[k,'parent_id']).json()
                pick = False
                picker = []    
                l_mf_pa = []
                mf_not_empty_pa = 0

                mf_childs_pa = mla_pa.get('main_features') if mla_pa.get('main_features') != None else 'null'

                if len(mf_childs_pa) > 0 and mf_childs_pa != 'null':
                    for mf in mf_childs_pa:
                        if mf.get('text') != '':
                            l_mf_pa = (str(l_mf_pa) + str(mf.get('text'))+"\n").replace('[]',"")
                            mf_not_empty_pa = mf_not_empty_pa + 1

                images = mla_pa.get('pictures')
                lista_url = []
                if images != None:
                    for im in range(len(images)):
                        if pick == False:
                            if images[im].get('suggested_for_picker') != []:
                                picker = images[im].get('suggested_for_picker')
                                pick = True
                            else:
                                picker = []                  
                    for im in images:
                        lista_url.append(im.get('url'))
                else:
                    parentProd = get_catalogProducts(mla_pa.get('id')).json().get('images')
                    for im in parentProd:
                        lista_url.append(im['url'])

                metafamilia.append({'id': existe_df.loc[k,'parent_id'].strip('MLA'),
                                    'site': 'MLA',
                                    'domain_id': mla_pa.get('domain_id'),
                                    'parent_id': existe_df.loc[k,'parent_id'],
                                    'status': mla_pa.get('status'),
                                    'Name': existe_df.loc[k,'name_parent'],
                                    'q_main_features': mf_not_empty_pa,
                                    'main_features': l_mf_pa,
                                    'suggested_for_picker': picker,                                               
                                    'q_picker': len(mla_pa.get('pickers')) if mla_pa.get('pickers') != None else 'null',
                                    'q_pictures': len(lista_url) if mla_pa.get('pictures') != None else 'null',
                                    'pictures': lista_url                                                                                             
                                })

            if existe_df.loc[k,'parent_id'].find("MLB") == 0:
                mlb_pa = get_product(existe_df.loc[k,'parent_id']).json()
                pick = False
                picker = []    
                l_mf_pa = []
                mf_not_empty_pa = 0

                mf_childs_pa = mlb_pa.get('main_features') if mlb_pa.get('main_features') != None else 'null'

                if len(mf_childs_pa) > 0 and mf_childs_pa != 'null':
                    for mf in mf_childs_pa:
                        if mf.get('text') != '':
                            l_mf_pa = (str(l_mf_pa) + str(mf.get('text'))+"\n").replace('[]',"")
                            mf_not_empty_pa = mf_not_empty_pa + 1

                images = mlb_pa.get('pictures') 
                lista_url = []
                if images != None:
                    for im in range(len(images)):
                        if pick == False:
                            if images[im].get('suggested_for_picker') != []:
                                picker = images[im].get('suggested_for_picker')
                                pick = True
                            else:
                                picker = []
                    for im in images:
                        lista_url.append(im.get('url'))
                else:
                    parentProd = get_catalogProducts(mlb_pa.get('id')).json().get('images')
                    for im in parentProd:
                        lista_url.append(im['url'])

                metafamilia.append({'id': existe_df.loc[k,'parent_id'].strip('MLB'),
                                    'site': 'MLB',
                                    'domain_id': mlb_pa.get('domain_id'),
                                    'parent_id': existe_df.loc[k,'parent_id'],
                                    'status': mlb_pa.get('status'),
                                    'Name': existe_df.loc[k,'name_parent'],
                                    'q_main_features': mf_not_empty_pa,
                                    'main_features': l_mf_pa,
                                    'suggested_for_picker': picker,                                               
                                    'q_picker': len(mlb_pa.get('pickers')) if mlb_pa.get('pickers') != None else 'null',
                                    'q_pictures': len(lista_url) if mlb_pa.get('pictures') != None else 'null',
                                    'pictures': lista_url                                                                                             
                                })

            if existe_df.loc[k,'parent_id'].find("MLM") == 0:
                mlm_pa = get_product(existe_df.loc[k,'parent_id']).json()
                pick = False
                picker = []    
                l_mf_pa = []
                mf_not_empty_pa = 0

                mf_childs_pa = mlm_pa.get('main_features') if mlm_pa.get('main_features') != None else 'null'

                if len(mf_childs_pa) > 0 and mf_childs_pa != 'null':
                    for mf in mf_childs_pa:
                        if mf.get('text') != '':
                            l_mf_pa = (str(l_mf_pa) + str(mf.get('text'))+"\n").replace('[]',"")
                            mf_not_empty_pa = mf_not_empty_pa + 1

                images = mlm_pa.get('pictures') 
                lista_url = []
                if images != None:
                    for im in range(len(images)):
                        if pick == False:
                            if images[im].get('suggested_for_picker') != []:
                                picker = images[im].get('suggested_for_picker')
                                pick = True
                            else:
                                picker = []
                    for im in images:
                        lista_url.append(im.get('url'))
                else:
                    parentProd = get_catalogProducts(mlm_pa.get('id')).json().get('images')
                    for im in parentProd:
                        lista_url.append(im['url'])

                metafamilia.append({'id': existe_df.loc[k,'parent_id'].strip('MLM'),
                                    'site': 'MLM',
                                    'domain_id': mlm_pa.get('domain_id'),
                                    'parent_id': existe_df.loc[k,'parent_id'],
                                    'status': mlm_pa.get('status'),
                                    'Name': existe_df.loc[k,'name_parent'],
                                    'q_main_features': mf_not_empty_pa,
                                    'main_features': l_mf_pa,
                                    'suggested_for_picker': picker,                                               
                                    'q_picker': len(mlm_pa.get('pickers')) if mlm_pa.get('pickers') != None else 'null',
                                    'q_pictures': len(lista_url) if mlm_pa.get('pictures') != None else 'null',
                                    'pictures': lista_url                                                                                             
                                })   
                
    metafamilia_df = pd.DataFrame(metafamilia).sort_values('Name').fillna('').reset_index(drop=True)

    print('\nBuscando las imágenes')
    for j in progressbar.progressbar(range(len(metafamilia_df))):
        imagenIds = metafamilia_df.loc[i,'pictures']
        for i in range(len(imagenIds)):
            metafamilia_df.loc[j,'imagen_'+str(i)] = str(imagenIds[i]).replace('-O','-F')

    del metafamilia_df['pictures']

    return metafamilia_df


def imagenProd (metafamilia_df):
    print("\nBuscando las imágenes")
    for imag in progressbar.progressbar(range(len(metafamilia_df))):
        im = 0
        if metafamilia_df.loc[imag,'children_ids'] != '':
            imagenProd = get_catalogProducts(metafamilia_df.loc[imag,'children_ids']).json().get('images')
            for image in imagenProd:
                metafamilia_df.loc[imag,'Imagen_'+str(im)] = str(image['url']).replace('-O','-F')
                im = im + 1
        else:
            imagenProd = get_catalogProducts(metafamilia_df.loc[imag,'parent_id']).json().get('images')
            for image in imagenProd:
                metafamilia_df.loc[imag,'Imagen_'+str(im)] = str(image['url']).replace('-O','-F')
                im = im + 1
    return metafamilia_df

def idsDomainCatalog(dom):
    lis = []
    ingreso = input('\nAnálisis para todos los sites: ').upper()
    if ingreso == 'SI':
        sites = ['MLA', 'MLB', 'MLM']
        for s in sites:
            domain = get_catalogDomains(s+'-'+dom, True)
            if domain.status_code in range(200,300):
                domain = domain.json()
                for d in domain:
                    if not d.get('id') in lis and not 'parent_id' in d: #GUARDA SOLO LOS HIJOS?
                        lis.append({'parent_id': d.get('id'),
                                    'name_parent': d.get('name'),
                                    'children_ids': d.get('children_ids')})
        existe_df = pd.DataFrame(lis).reset_index(drop=True)
    elif ingreso == 'NO':
        sites = []
        try:
            for _ in range(int(input('\nIngrese la cantidad de sites a análizar: '))):
                sites.append(input("\nIngrese un sites: ").upper())
        except ValueError:
            print("\x1b[0;31m\nError, ingrese solamente numeros\x1b[0;37m")

        for s in sites:
            domain = get_catalogDomains(s+'-'+dom, True)
            if domain.status_code in range(200,300):
                domain = domain.json()
                for d in domain:
                    if not d.get('id') in lis and not 'parent_id' in d:
                        lis.append({'parent_id': d.get('id'),
                                    'name_parent': d.get('name'),
                                    'children_ids': d.get('children_ids')})

        existe_df = pd.DataFrame(lis).reset_index(drop=True)
    else:
        print("\x1b[0;31m\nError, ingrese SI o NO\x1b[0;37m")

    return existe_df

def opciones():
    print('\x1b[0;36mOpciones disponibles:\x1b[0;37m\n')
    print('\t1. Investigación (Análisis oferta y atributos, wordcloud, preguntas y rank x brand')
    print('\t2. Análisis oferta y rank x brand')
    print('\t3. Análisis atributos')
    print('\t4. Wordcloud y preguntas')
    print('\t0. Salir')

def opciones_sites():
    print('\x1b[0;36mOpciones disponibles:\x1b[0;37m\n')
    print('\t1. Análisis para 3 sites (MLA, MLB y MLM)')
    print('\t2. Análisis para 6 sites (MLA, MLB, MLM, MLU, MLC y MCO)')

def menu():
    while True:
        opciones()
        try:
            entrada_usuario = int(input("\nSeleccione una opcion: "))
            if entrada_usuario in range(5):
                if entrada_usuario == 1:                                       
                    try:
                        #TERADATA
                        username_teradata = input('\nIngrese el user Meli: ').lower()
                        password_teradata = getpass.getpass('Ingrese la clave Meli: ')
                        session = teradata_session(username_teradata, password_teradata)
                        
                        domain = input("\x1b[0;37m\nIngrese el dominio a analizar: \x1b[0;37m").upper()
                        att_domain = attributes_domain(domain)
                        
                        
                        #Análisis para los 3 sites o para los 6 sites
                        opciones_sites()
                        option_sites = int(input("\nSeleccione una opcion: "))

                        if option_sites == 1:
                            flag = 1
                            df_items = pd.DataFrame()
                            sites = ['MLA', 'MLB', 'MLM']
                            for s in sites:
                                items = query_new(s, domain, session)
                                df_items = pd.concat([df_items,items], sort=False).reset_index(drop = True)
                            
                            df_oferta, df_brands = analisisOferta(df_items)

                            pregs_ar, pregs_mx, pregs_pt, pregs_cl, pregs_co, pregs_ur= preguntas(df_items) #Por cada sitio reprocesa las preguntas
                            load_wc(domain, pregs_ar, pregs_mx , pregs_pt, pregs_cl, pregs_co, pregs_ur, flag)
                            print("Se cargaron los Wordcloud en el siguiente drive https://drive.google.com/drive/u/0/folders/1zwqLW9Meq4rmIZIEv0VFNREC5K58QlmC")

                        elif option_sites == 2:
                            flag = 2
                            df_items = pd.DataFrame()
                            sites = ['MLA', 'MLB', 'MLM','MLC','MCO','MLU']
                            for s in sites:
                                items = query_new(s, domain, session)
                                df_items = pd.concat([df_items,items], sort=False).reset_index(drop = True)
                            
                            pregs_ar, pregs_mx , pregs_pt, pregs_cl, pregs_co, pregs_ur= preguntas(df_items)
                            pregs_ar = pregs_ar.fillna('')
                            df_oferta, df_brands = analisisOferta(df_items)

                            load_wc(domain, pregs_ar, pregs_mx , pregs_pt, pregs_cl, pregs_co, pregs_ur,flag)
                            print("Se cargaron los Wordcloud en el siguiente drive https://drive.google.com/drive/u/0/folders/1zwqLW9Meq4rmIZIEv0VFNREC5K58QlmC")

                        else:
                            cadena = "Error al ingresar valor.\n-----\nProceso infalizado"
                            print("\x1b[0;35m\n"+cadena.center(100, "-")+"\x1b[0;37m")
                            break


                        Planilla = input('\x1b[0;33mIngrese ID de planilla para guardar los datos: \x1b[0;37m')
                        
                        credentials = creden()[0]
                        d2g = creden()[3]

                        d2g.upload(att_domain, Planilla, str('Atributos'), credentials=credentials, row_names=False, new_sheet_dimensions=(len(att_domain), len(att_domain.columns)))
                        print("Se creo la hoja Atributos_"+domain)

                        d2g.upload(df_oferta.fillna(''), Planilla, str('Análisis Oferta ML_'+ domain), credentials=credentials, row_names=False, new_sheet_dimensions=(len(df_oferta), len(df_oferta.columns)))
                        print("Se creo la hoja Análisis Oferta ML_"+domain)

                        d2g.upload(df_brands, Planilla, str('Brands'), credentials=credentials, row_names=False, new_sheet_dimensions=(len(df_brands), len(df_brands.columns)))
                        print("Se creo la hoja Brands")

                        df_pregs = pd.concat([pregs_ar, pregs_mx, pregs_pt,pregs_cl,pregs_co,pregs_ur], sort=False).reset_index(drop = True)

                        d2g.upload(df_pregs, Planilla, str('Preguntas'), credentials=credentials, row_names=False, new_sheet_dimensions=(len(df_pregs), len(df_pregs.columns)))
                        print("Se creo la hoja Preguntas")

                    except requests.exceptions.ConnectionError:
                        cadena = "CONECTATE A LA VPN"
                        print("\x1b[0;35m\n"+cadena.center(100, "-")+"\x1b[0;37m")
                        break

                if entrada_usuario == 2:
                    try:
                        #TERADATA
                        username_teradata = input('\nIngrese el user Meli: ').lower()
                        password_teradata = getpass.getpass('Ingrese la clave Meli: ')
                        session = teradata_session(username_teradata, password_teradata)
                        
                        domain = input("\x1b[0;37m\nIngrese el dominio a analizar: \x1b[0;37m").upper()
                        att_domain = attributes_domain(domain)
                        
                        
                        #Análisis para los 3 sites o para los 6 sites
                        opciones_sites()
                        option_sites = int(input("\nSeleccione una opcion: "))

                        if option_sites == 1:
                            flag = 1
                            df_items = pd.DataFrame()
                            sites = ['MLA', 'MLB', 'MLM']
                            for s in sites:
                                items = query_new(s, domain, session)
                                df_items = pd.concat([df_items,items], sort=False).reset_index(drop = True)
                            
                            df_oferta, df_brands = analisisOferta(df_items)
                            
                        elif option_sites == 2:
                            flag = 2
                            df_items = pd.DataFrame()
                            sites = ['MLA', 'MLB', 'MLM','MLC','MCO','MLU']
                            for s in sites:
                                items = query_new(s, domain, session)
                                df_items = pd.concat([df_items,items], sort=False).reset_index(drop = True)

                            df_oferta, df_brands = analisisOferta(df_items)

                        else:
                            cadena = "Error al ingresar valor.\n-----\nProceso infalizado"
                            print("\x1b[0;35m\n"+cadena.center(100, "-")+"\x1b[0;37m")
                            break

                        Planilla = input('\x1b[0;33mIngrese ID de planilla para guardar los datos: \x1b[0;37m')
                        
                        credentials = creden()[0]
                        d2g = creden()[3]

                        d2g.upload(df_oferta.fillna(''), Planilla, str('Análisis Oferta ML_'+ domain), credentials=credentials, row_names=False, new_sheet_dimensions=(len(df_oferta), len(df_oferta.columns)))
                        print("Se creo la hoja Análisis Oferta ML_"+domain)

                        d2g.upload(df_brands, Planilla, str('Brands'), credentials=credentials, row_names=False, new_sheet_dimensions=(len(df_brands), len(df_brands.columns)))
                        print("Se creo la hoja Brands")


                    except requests.exceptions.ConnectionError:
                        cadena = "CONECTATE A LA VPN"
                        print("\x1b[0;35m\n"+cadena.center(100, "-")+"\x1b[0;37m")
                        break

                if entrada_usuario == 3:
                    try:
                        domain = input("\x1b[0;37m\nIngrese el dominio a analizar: \x1b[0;37m").upper()

                        att_domain = attributes_domain(domain)

                        Planilla = input('\x1b[0;33mIngrese ID de planilla para guardar los datos: \x1b[0;37m')
                        
                        credentials, gc, open_sheet, d2g = creden()

                        d2g.upload(att_domain, Planilla, str('Atributos'), credentials=credentials, row_names=False, new_sheet_dimensions=(len(att_domain), len(att_domain.columns)))
                        print("Se creo la hoja Atributos_"+domain)

                    except requests.exceptions.ConnectionError:
                        cadena = "CONECTATE A LA VPN"
                        print("\x1b[0;35m\n"+cadena.center(100, "-")+"\x1b[0;37m")
                        break

                if entrada_usuario == 4:
                    try:
                    #TERADATA
                        username_teradata = input('\nIngrese el user Meli: ').lower()
                        password_teradata = getpass.getpass('Ingrese la clave Meli: ')
                        session = teradata_session(username_teradata, password_teradata)
                        
                        domain = input("\x1b[0;37m\nIngrese el dominio a analizar: \x1b[0;37m").upper()
                        
                        #Análisis para los 3 sites o para los 6 sites
                        opciones_sites()
                        option_sites = int(input("\nSeleccione una opcion: "))

                        if option_sites == 1:
                            flag = 1
                            df_items = pd.DataFrame()
                            sites = ['MLA', 'MLB', 'MLM']
                            for s in sites:
                                items = query_new(s, domain, session)
                                df_items = pd.concat([df_items,items], sort=False).reset_index(drop = True)
                            
                            pregs_ar, pregs_mx, pregs_pt, pregs_cl, pregs_co, pregs_ur= preguntas(df_items) #Por cada sitio reprocesa las preguntas
                            
                            load_wc(domain, pregs_ar, pregs_mx , pregs_pt, pregs_cl, pregs_co, pregs_ur, flag)
                            print("Se cargaron los Wordcloud en el siguiente drive https://drive.google.com/drive/u/0/folders/1zwqLW9Meq4rmIZIEv0VFNREC5K58QlmC")

                        elif option_sites == 2:
                            flag = 2
                            df_items = pd.DataFrame()
                            sites = ['MLA', 'MLB', 'MLM','MLC','MCO','MLU']
                            for s in sites:
                                items = query_new(s, domain, session)
                                df_items = pd.concat([df_items,items], sort=False).reset_index(drop = True)
                            
                            pregs_ar, pregs_mx , pregs_pt, pregs_cl, pregs_co, pregs_ur= preguntas(df_items)
                            
                            load_wc(domain, pregs_ar, pregs_mx , pregs_pt, pregs_cl, pregs_co, pregs_ur,flag)
                            print("Se cargaron los Wordcloud en el siguiente drive https://drive.google.com/drive/u/0/folders/1zwqLW9Meq4rmIZIEv0VFNREC5K58QlmC")

                        else:
                            cadena = "Error al ingresar valor.\n-----\nProceso infalizado"
                            print("\x1b[0;35m\n"+cadena.center(100, "-")+"\x1b[0;37m")
                            break

                        Planilla = input('\x1b[0;33mIngrese ID de planilla para guardar los datos: \x1b[0;37m')
                        
                        credentials = creden()[0]
                        d2g = creden()[3]

                        df_pregs = pd.concat([pregs_ar, pregs_mx, pregs_pt,pregs_cl,pregs_co,pregs_ur], sort=False).reset_index(drop = True)

                        d2g.upload(df_pregs, Planilla, str('Preguntas'), credentials=credentials, row_names=False, new_sheet_dimensions=(len(df_pregs), len(df_pregs.columns)))
                        print("Se creo la hoja Preguntas")

                    except requests.exceptions.ConnectionError:
                        cadena = "CONECTATE A LA VPN"
                        print("\x1b[0;35m\n"+cadena.center(100, "-")+"\x1b[0;37m")
                        break
                else:
                    cadena = "Finalizo el programa"
                    print("\x1b[1;31m"+cadena.center(100, "_")+"\x1b[0;37m\n")
                    break
            else:
                print('\x1b[0;31m\nError, solo de aceptan numeros del 0 al 4\n\x1b[0;37m')

        except ValueError:
            print("\x1b[0;31m\nError, ingrese solamente numero\x1b[0;37m")

if __name__ == '__main__':
    menu()