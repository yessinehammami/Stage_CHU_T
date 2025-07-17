import pandas as pd
import numpy as np

#MATRICE UF-EM
df_matrice=pd.read_excel(r'C:\Stage\data-original\Matrice UF_EM\Matrice UF EM au 211024.xlsx')
cols_to_convert = ['NumUA', 'NumEM CVG']
df_matrice[cols_to_convert] = df_matrice[cols_to_convert].astype(str)



def clean (df):
    """
    Netoyage du fichier RSA et rétention des colonnes qui nous intéressent.

    Arg :
      df: table rsa d'une année précise example rsa_2024

    Returns:
        df avec rsa nettoyé

    """
    columns_to_keep= ['Numéro de séquence',
                    'Unité médicale (Code)',
                    'Equipe médicale (Code - Libellé)',
                    'Durée de séjour brute',
                    'GHS (Code)',
                    "Date d'entrée",
                    'Type Séjour'
                    ]
    df=df[columns_to_keep]

    df=df.drop(0).reset_index(drop=True)

    df["Date d'entrée"] = pd.to_datetime(df["Date d'entrée"], dayfirst=True)
    df['date sortie uf'] = df["Date d'entrée"] + pd.to_timedelta(df['Durée de séjour brute']-1, unit='D')

    df['Libellé équipe médicale'] = df['Equipe médicale (Code - Libellé)'].str[6:].str[:-61]
    df['Equipe médicale (Code - Libellé)']=df['Equipe médicale (Code - Libellé)'].str[:4]

    df.rename(columns={'Unité médicale (Code)': 'Code UF',
                            'Equipe médicale (Code - Libellé)': 'Equipe médicale',
                            "Date d'entrée" : 'date entrée'
                            },
                            inplace=True)

    cols_to_convert = ['Equipe médicale', 'Code UF', 'GHS (Code)']
    df['Code UF'] = df['Code UF'].astype(int)
    df[cols_to_convert] = df[cols_to_convert].astype(str)

    return df

    
def générer_rsa_réel(rsa_total,année):
    
    """
    Génération du rsa d'une année à partir du rsa total de toutes les années disponibles.

    Arg :
      rsa_total : le fichier rsa résultant du concaténation de tous les rsa séparés

    Returns:
        nouveau rsa pour l'année concernée
    """

    rsa_total["date entrée"] = pd.to_datetime(rsa_total["date entrée"],format='%Y-%m-%d')
    rsa_total["date sortie uf"] = pd.to_datetime(rsa_total["date sortie uf"],format='%Y-%m-%d')

    rsa= rsa_total[(rsa_total['date entrée'].dt.year == année)
                    | 
                    (rsa_total['date sortie uf'].dt.year == année)]
    return rsa


#GENERER LES LITS OCCUPES REELEMENT
def generer_lit(rsa, année):
    """
    Génere pour une année la table lit : nombre de lits occupés pour chaque uf pa jour

    Args:
        rsa : table rsa nettoyé de l'année concernée
        année : l'année du fichier rsa fourni

    Returns :
        table lit
    """

    start = f"{année}-01-01"
    end = f"{année}-12-31"
    date_range = pd.date_range(start=start, end=end, freq="D")

    list_uf=rsa['Code UF'].unique().tolist()

    df_lit = pd.DataFrame(columns=list_uf)
    df_lit['Date']= date_range
    df_lit.set_index('Date', inplace=True)
    df_lit[:] = 0
    for k in list_uf:
        
        df_uf=rsa[rsa['Code UF']== k]
        df_uf = df_uf.sort_values(by='date entrée', ascending=True).reset_index(drop=True)

        for index, row in df_uf.iterrows():
            mask = (df_lit.index >= row['date entrée']) & (df_lit.index <= row['date sortie uf'])
            df_lit.loc[mask, k] += 1
    return df_lit


def is_heb(row):
    """
    identifier les hébergements.

    Arg :
      ligne dans un rsa 

    Returns:
        hebergement ou non 
    """
    # Get the list of 'NumUA' values for that Equipe médicale
    valid_ufs = df_matrice.loc[df_matrice['NumEM CVG'] == row['Equipe médicale'], 'NumUA'].tolist()
    return row['Code UF'] not in valid_ufs




#GENERER LE BESOIN REEL AVEC LA REAFFECTATION DES EM
def besoin_lit(df_rsa, lit):

    

    """
    Génre une table qui contient 5 colonnes ;Date , UF, max, min, médiane.
    Suite à la réaffectation des em plusieurs fois (100 fois) randomisés selon une probabilité pré-calculé,
    on obtient pour chaque uf et à chaque date plusieurs valeurs de lits occupés, on extrait max min médiane

    Args:
    df_rsa: table rsa pour l'année concernée
    lit: table lit déja calculé pour l'année concernée

    Returns:
        Table décrite dans la définition
    """
    columns_to_keep= ['Code UF',
                    'Equipe médicale',
                    'GHS (Code)',
                    'date entrée',
                    'date sortie uf',
                    'Type Séjour'
                    ]
    df_rsa = df_rsa[columns_to_keep]
    df_rsa['Code UF']= df_rsa['Code UF'].astype(str)
    df_rsa['Equipe médicale']= df_rsa['Equipe médicale'].astype(str)

    df_heb = df_rsa[df_rsa.apply(is_heb, axis=1)]

    df_no_heb = df_rsa.drop(df_heb.index)
    
    # MELT THE lit df
  
    lit_reset= lit.reset_index()
    melted_lit = lit_reset.melt(id_vars='Date', var_name='Code UF', value_name='lits occupés')

    #PROCESS DE REAFFECTATION : 3 ETAPES

        #POUR CHAQUE HEB HORS UF TROUVER LES UF POSSIBLES ET LEURS PROBABILITES SELON L'OCCURENCE

    df_heb['possible_UF'] = [[] for _ in range(len(df_heb))]
    df_heb['p'] = [[] for _ in range(len(df_heb))]
    df_heb['randomly_selected_uf']=0
        
    for index, row in df_heb.iterrows():
        
        dfi = df_no_heb[
        (df_no_heb['Equipe médicale'] == row['Equipe médicale']) &
        (df_no_heb['GHS (Code)'] == row['GHS (Code)']) &
        (df_no_heb['Type Séjour'] == row['Type Séjour'])]

        counts3_UF_ass = dfi.groupby(['Equipe médicale','GHS (Code)','Type Séjour','Code UF']).size().reset_index(name='count3')

        total = counts3_UF_ass['count3'].sum()
        counts3_UF_ass['p']=0

        if not(counts3_UF_ass.empty or total == 0):

            counts3_UF_ass['p']=(counts3_UF_ass['count3'] / total)
            df_heb.at[index, 'possible_UF'] = counts3_UF_ass['Code UF'].tolist()
            df_heb.at[index, 'p'] = counts3_UF_ass['p'].tolist()

        else:
            df_heb.at[index, 'possible_UF'] = list([row['Code UF']])
            df_heb.at[index, 'p'] = list([1])
        print(index)
          
    
    #EFFECTUER LES ESSAIS RANDOMISES SELON LA PROBABILITE CALCULE

    random_trials = {}
    for trial in range(10):
            df_heb['randomly_selected_uf'] = df_heb.apply(
            lambda row: np.random.choice(row['possible_UF'], p=np.array(row['p'])/np.sum(row['p'])),
            axis=1
            )
            random_trials[trial] = df_heb.drop(columns=['possible_UF', 'p'])

        #UPDATE df lit déja extaite avec les heb hors matrice    

    for i , df in random_trials.items():
            col_name = f'besoin_trial_{i}'
            melted_lit[col_name]=melted_lit['lits occupés']

            for index, row in df.iterrows():
                melted_lit.loc[
                (melted_lit['Date'] >= row['date entrée']) &
                (melted_lit['Date'] <= row['date sortie uf']) &
                (melted_lit['Code UF'] == row['Code UF']),
                col_name
                ] -= 1
                        
                melted_lit.loc[
                (melted_lit['Date'] >= row['date entrée']) &
                (melted_lit['Date'] <= row['date sortie uf']) &
                (melted_lit['Code UF'] == row['randomly_selected_uf']),
                col_name
                ] += 1
            
        
        #EXTRAIRE MAX MIN MEDIANE DE CES ESSAIS RANDOMISES
    besoin_cols = [col for col in melted_lit.columns if col.startswith('besoin_trial_')]

    melted_lit['max'] = melted_lit[besoin_cols].max(axis=1)
    melted_lit['min'] = melted_lit[besoin_cols].min(axis=1)
    melted_lit['médiane'] = melted_lit[besoin_cols].median(axis=1)

    melted_lit = melted_lit[['Date','Code UF','max', 'min', 'médiane']]

    return melted_lit


def charge_em_um (rsa, année):

    date_range = pd.date_range(start=f"{année}-01-01", end=f"{année}-12-31", freq="D")

    list_uf= rsa['Code UF'].unique().tolist()
    list_em = rsa['Equipe médicale'].unique().tolist()

    multi_cols = pd.MultiIndex.from_product([list_em, list_uf], names=['Equipe médicale', 'Code UF'])

    dfn = pd.DataFrame(0,index=date_range, columns=multi_cols)

    for em in list_em:
        for uf in list_uf:
            df_em_uf = rsa[(rsa['Equipe médicale'] == em) & (rsa['Code UF']==uf)]
            for index, row in df_em_uf.iterrows():
                dfn.loc[
                    (dfn.index >= row['date entrée']) & (dfn.index <= row['date sortie uf']),  # Select date range in index
                    pd.IndexSlice[em, uf]
                ] += 1


    dfn = dfn.loc[:, (dfn != 0).any(axis=0)]

    return dfn