#!/usr/bin/env python

import pandas as pd
import redcap
import params
import tokens

def get_SAE_events():
    """
    Function used to run and check all SAEs from ICARIA and get a list of errors
    and potential errors in SAE numeration, parallelly it also checks potential
    ghost entry errors related with SAEs.

    :return: It used to save two different files:
        - potential_errors_ids.csv: Potential errors in SAE numeration.
        - ghost_entries.csv: Potential ghost entry errors detected on SAEs.
    """
    all_saes = pd.DataFrame()
    ghost_df = pd.DataFrame()
    potential_errors_cases = pd.DataFrame(columns=["record_id", "SAE_instance"])

    for project_key in tokens.REDCAP_PROJECTS_ICARIA:
        print(project_key)
        project = redcap.Project(tokens.URL, tokens.REDCAP_PROJECTS_ICARIA[project_key])
        df = project.export_records(format_type='df', fields=params.SAE_FIELDS,events=['adverse_events_arm_1'])
        dfres = df.reset_index()[['record_id','redcap_repeat_instance','sae_number','sae_report_type',
                                  'sae_onset','sae_date','sae_hosp_admin_date','sae_death_date',
                                  'sae_hosp_disch_date','sae_complete','sae_interviewer_id']]
        dict_sn = {}
        dfsn = project.export_records(format_type='df', fields=['study_number'], events=['epipenta1_v0_recru_arm_1'])
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

        sae_numbers = []
        for name,group in dfgroup:
            list_admis = list(group['sae_hosp_admin_date'])
            list_death = list(group['sae_death_date'])

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

                elif c >= 2 and (list_admis[c] == list_admis[c-2] or list_death[c] == list_death[c-2]):
                    sae_number = dict_sn[name] + "-0" + str(sae_n_list[c-2])
                    sae_n_list.append(sae_n_list[c-2])
                    potential_errors_cases.loc[len(potential_errors_cases)]=el['record_id'], el['redcap_repeat_instance']

                elif c >= 3 and (list_admis[c] == list_admis[c-3] or list_death[c] == list_death[c-3]):
                    sae_number = dict_sn[name] + "-0" + str(sae_n_list[c-3])
                    sae_n_list.append(sae_n_list[c-3])
                    potential_errors_cases.loc[len(potential_errors_cases)]=el['record_id'], el['redcap_repeat_instance']

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

        """DEPRECATED: This part automatically changed the numeration errors in REDCap"""
#        if to_import_list != []:
#            response = project.import_records(to_import_list)
#            print("[SAE numeration] Numeration setup: {}".format(response.get('count')))

    diff = []
    for k,el in all_saes.T.items():
        if el['sae_number'] != el['new_number']:
            diff.append(True)
        else:
            diff.append(False)

    print("\nSAE NUMERATION ERRORS:")
    print(all_saes[diff])
    print("\nOTHER POTENCIAL SAE NUMERATION ERRORS:")
    print(potential_errors_cases)
    print("\nPOTENCIAL GHOST ENTRY ERRORS RELATED WITH SAEs:")
    print(ghost_df)

    ghost_df.to_csv(tokens.errors_ghost)
    potential_errors_cases.to_csv(tokens.errors_potental)