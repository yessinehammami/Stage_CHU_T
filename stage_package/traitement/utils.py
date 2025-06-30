import pandas as pd

def traiter_RH(chemin_fichier_RH, année):
    
    #On lit le fichier
    df=pd.read_excel(chemin_fichier_RH)

    #On garde les entrées correspondant à l'année choisie
    iso_calendar = df['Date'].dt.isocalendar()  
    df = df[iso_calendar['year'] == année]

    df.fillna(0, inplace=True)
    df = df.apply(lambda x: x.astype(int) if x.dtype == 'float' else x)

    df = pd.concat([df['Date'].dt.isocalendar().week.rename('Semaine'), df], axis=1)
    df = df.drop(columns=['Code Métier','Date'])

    result = df.groupby(['Semaine','Code UA','Métier'])[['NB d\'agents présents', 'NB d\'agents absence imprévue', 'Nb d\'agents absence prévue']].sum()
    result['effectif_total'] = result['NB d\'agents absence imprévue']+result['Nb d\'agents absence prévue']+ result['NB d\'agents présents']
    result['absences totales']=result['NB d\'agents absence imprévue']+result['Nb d\'agents absence prévue']
    result = result.reset_index()
    result.set_index('Semaine', inplace=True)
    result = result.rename(columns={'Code UA': 'Code UF'})

    return result

def traiter_lits(chemin_fichier_lit, année):
    df= pd.read_excel(chemin_fichier_lit,skiprows=1)
    df['Code UF'] = df['CODE - LIBELLE UF'].str[:4]
    df['Libelle UF'] = df['CODE - LIBELLE UF'].str[5:]
    df = df.drop(df.columns[0], axis=1)
    dfg=pd.DataFrame()
    c=0
    for i in range(0,225):
        k=0
        for j in range(0,52):
            dfg.loc[c,'Semaine'] = j+1
            dfg.loc[c,'Code UF']= df.loc[i,"Code UF"]
            dfg.loc[c,'LITS INSTALLES'] = df.iloc[i, k]
            dfg.loc[c,'Lits fermés max prévisionnels'] = df.iloc[i, k+1]
            dfg.loc[c,'Journées lits fermées 2024'] = df.iloc[i, k+2]
            dfg.loc[c,'Lits fermés moyens'] = df.iloc[i, k+3]
            k=k+4
            c=c+1

    return dfg