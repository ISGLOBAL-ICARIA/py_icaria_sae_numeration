#!/usr/bin/env python
""" Python script to manage different components of the reporting of Serious Adverse Events (SAEs) in the ICARIA
Clinical Trial. These components are: (1) SAE numbering, etc."""

import csv
import pandas as pd
import redcap
import params
import tokens


def get_SAE_events():
    all_saes = pd.DataFrame()
    onsets = pd.DataFrame(columns=["record_id", "SAE_instance"])
    ghost_df = pd.DataFrame()
    potential_errors_cases = pd.DataFrame(columns=["record_id", "SAE_instance"])

    for project_key in tokens.REDCAP_PROJECTS_ICARIA:
        print(project_key)
        project = redcap.Project(tokens.URL, tokens.REDCAP_PROJECTS_ICARIA[project_key])
        df = project.export_records(format='df', fields=params.SAE_FIELDS,events=['adverse_events_arm_1'])
        dfres = df.reset_index()[['record_id','redcap_repeat_instance','sae_number','sae_report_type',
                'sae_onset','sae_date','sae_hosp_admin_date','sae_death_date',
                'sae_hosp_disch_date','sae_complete','sae_interviewer_id']]
        dict_sn = {}
        dfsn = project.export_records(format='df', fields=['study_number'], events=['epipenta1_v0_recru_arm_1'])
        dfsn = dfsn.reset_index().set_index('record_id')

        for k,el in dfsn.T.items():
            dict_sn[k] = el['study_number']

        ghost_saes = dfres[~dfres['sae_interviewer_id'].notnull()]
        if ghost_df.empty:
            ghost_df = ghost_saes
        else:
            ghost_df = pd.concat([ghost_df,ghost_saes])

        dfres = dfres[dfres['sae_interviewer_id'].notnull()]
        dfres = dfres[(dfres['sae_interviewer_id'].notnull())&((dfres['sae_complete']==2))]
        dfgroup = dfres.groupby('record_id')

        #print(dfgroup)
        sae_numbers = []
        for name,group in dfgroup:
            #print(name,group)
            list_admis = list(group['sae_hosp_admin_date'])
            list_onset = list(group['sae_onset'])
            list_death = list(group['sae_death_date'])
            #print(list_admis)

            c = 0
            sae_n = 0
            sae_n_list = []
            for k, el in group.T.items():
                if c == 0:
                    sae_n = 1
                    sae_n_list.append(sae_n)
                    sae_number = dict_sn[name] + "-0" + str(sae_n)
                elif list_admis[c] == list_admis[c-1] or list_death[c] == list_death[c-1]:
                    sae_n_list.append(sae_n)
                    sae_number = dict_sn[name] + "-0" + str(sae_n)

                #elif list_onset[c] == list_onset[c-1]:
                #    sae_n_list.append(sae_n)
                #    sae_number = dict_sn[name] + "-0" + str(sae_n)
                #    onsets.loc[len(onsets)]=el['record_id'], el['redcap_repeat_instance']
                    #print("hhhhhhhhhhhhhhhhhh")


                elif c >= 2 and (list_admis[c] == list_admis[c-2] or list_death[c] == list_death[c-2]):
                    sae_number = dict_sn[name] + "-0" + str(sae_n_list[c-2])
                    sae_n_list.append(sae_n_list[c-2])
                    potential_errors_cases.loc[len(potential_errors_cases)]=el['record_id'], el['redcap_repeat_instance']
                    #print(potential_errors_cases)

                #elif c >= 2 and (list_onset[c] == list_onset[c-2]):
                #    sae_number = dict_sn[name] + "-0" + str(sae_n_list[c - 2])
                #    sae_n_list.append(sae_n_list[c - 2])
                    #potential_errors_cases.loc[len(potential_errors_cases)]=el['record_id'] + el['redcap_repeat_instance']
                #    onsets.loc[len(onsets)]=el['record_id'], el['redcap_repeat_instance']

                elif c >= 3 and (list_admis[c] == list_admis[c-3] or list_death[c] == list_death[c-3]):
                    sae_number = dict_sn[name] + "-0" + str(sae_n_list[c-3])
                    sae_n_list.append(sae_n_list[c-3])
                    potential_errors_cases.loc[len(potential_errors_cases)]=el['record_id'], el['redcap_repeat_instance']

                #elif c >= 3 and (list_onset[c] == list_onset[c-3]):
                #    sae_number = dict_sn[name] + "-0" + str(sae_n_list[c-3])
                #    sae_n_list.append(sae_n_list[c-3])
                #    onsets.loc[len(onsets)]=el['record_id'], el['redcap_repeat_instance']
                else:
                    sae_n = max(sae_n_list)+1
                    sae_n_list.append(sae_n)
                    sae_number = dict_sn[name] + "-0" + str(sae_n)

                sae_numbers.append(sae_number)
                c += 1

        dfres['new_number'] = sae_numbers

        new_numb = dfres[['record_id','redcap_repeat_instance','sae_number','new_number']]
        diff = []
        for k, el in new_numb.T.items():
            if el['sae_number'] != el['new_number']:
                diff.append(True)
            else:
                diff.append(False)
        new_numb = new_numb[diff]

        if not new_numb.empty:
            #print(new_numb)
            new_numb.to_csv(tokens.changes_folder+str(project_key)+".csv")

        if all_saes.empty:
            all_saes =  dfres[['record_id','redcap_repeat_instance','sae_number','new_number']]
        else:
            all_saes = pd.concat([all_saes,dfres[['record_id','redcap_repeat_instance','sae_number','new_number']]])

        to_import_list = []
        if not new_numb.empty:
            new_numb = new_numb[~new_numb['record_id'].isin(params.blocked_records)]

        for k, el in new_numb.T.items():
            to_import_list.append({
                'record_id': el['record_id'],
                'redcap_event_name': 'adverse_events_arm_1',
                'redcap_repeat_instrument': 'sae',
                'redcap_repeat_instance': el['redcap_repeat_instance'],
                'sae_number': el['new_number']})
#        print(to_import_list)


        if to_import_list != []:
            response = project.import_records(to_import_list)
            print("[SAE numeration] Numeration setup: {}".format(response.get('count')))


    diff = []
    for k,el in all_saes.T.items():
        if el['sae_number'] != el['new_number']:
            diff.append(True)
        else:
            diff.append(False)

    print(all_saes[diff])

    print("POTENCIAL ERROR CASES")
    print(potential_errors_cases)

    ghost_df.to_csv(tokens.errors_ghost)
    potential_errors_cases.to_csv(tokens.errors_potental)

def get_files():
    dfdeaths_all = pd.DataFrame()
    dfsn_all = pd.DataFrame()
    dfres_all = pd.DataFrame()

    for project_key in tokens.REDCAP_PROJECTS_ICARIA:
        print(project_key)
        project = redcap.Project(tokens.URL, tokens.REDCAP_PROJECTS_ICARIA[project_key])
        df = project.export_records(format='df', fields=params.SAE_FIELDS,events=['adverse_events_arm_1'])
        dfres = df.reset_index()[['record_id','redcap_repeat_instance','sae_number','sae_report_type',
                'sae_onset','sae_date','sae_hosp_admin_date','sae_death','sae_death_date', 'sae_outcome',
                'sae_hosp_disch_date','sae_complete','sae_interviewer_id']]
        dfsn = project.export_records(format='df', fields=['study_number'], events=['epipenta1_v0_recru_arm_1'])
        dfsn = dfsn.reset_index().set_index('record_id')

        dfdeaths = project.export_records(format='df', fields=[
            'death_reported_date','death_place','death_date',
            'death_interviewer_id'], events=['end_of_fu_arm_1'])
        print(dfdeaths)

        if dfdeaths_all.empty:
            dfdeaths_all = dfdeaths
        else:
            dfdeaths_all = pd.concat([dfdeaths_all,dfdeaths])

        if dfsn_all.empty:
            dfsn_all = dfsn
        else:
            dfsn_all = pd.concat([dfsn_all,dfsn])

        if dfres_all.empty:
            dfres_all = dfres
        else:
            dfres_all = pd.concat([dfres_all,dfres])
    dfres_all.to_csv(tokens.dfres_all_file)
    dfdeaths_all.to_csv(tokens.dfdeaths_all_file)
    dfsn_all.to_csv(tokens.dfsn_all_file)

def info_sae():
    dfsn = pd.read_csv(tokens.dfsn_all_file)
    dfres = pd.read_csv(tokens.dfres_all_file)
    dfdeaths = pd.read_csv(tokens.dfdeaths_all_file)
    dfdeaths = dfdeaths[dfdeaths['death_interviewer_id'].notnull()]

    dfdeaths_ids = dfdeaths['record_id'].unique()
    dfres = dfres.set_index('record_id')

    dfres_deaths_ids = dfres['sae_death_date'].dropna().reset_index()['record_id'].unique()
    print(dfres_deaths_ids)
    dfres_deaths_ids = dfres[(dfres['sae_death']==1)|(dfres['sae_outcome']==7)].reset_index()['record_id'].unique()
    print(dfres_deaths_ids)
    print("Deaths in SAE not in daeth form report: " + str(list(set(dfres_deaths_ids).difference(dfdeaths_ids))))
    print("Deaths not as SAE report: " + str(list(set(dfdeaths_ids).difference(dfres_deaths_ids))))

    deaths_wo_sae = dfdeaths[dfdeaths['record_id'].isin(list(set(dfdeaths_ids).difference(dfres_deaths_ids)))]

    sn_deaths_wo_sae = []
    for el in deaths_wo_sae['record_id']:
        sn_deaths_wo_sae.append(dfsn[dfsn['record_id']==el]['study_number'].values[0])
    deaths_wo_sae['study_number'] = sn_deaths_wo_sae
    deaths_wo_sae[['record_id', 'study_number','death_reported_date', 'death_place','death_interviewer_id', 'death_date']].to_csv(tokens.general_folder+"results/deaths_wo_sea.csv",index=False)
