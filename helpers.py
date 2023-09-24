import datetime
import pandas as pd
import re
import numpy as np
from dateutil.relativedelta import relativedelta


def convert_to_datetime(created_date):
    if len(str(created_date)) < 10:
        return np.nan
    else:
        return datetime.date(int(created_date[:4]), int(created_date[5:7]), int(created_date[8:10]))


def convert_to_timestamp(created_date):
    if len(str(created_date)) < 10:
        return np.nan
    else:
        year = int(created_date[:4])
        month = int(created_date[5:7])
        day = int(created_date[8:10])
        try:
            hour = int(created_date[11:13])
            minute = int(created_date[14:16])
            second = int(created_date[17:19])
        except:
            hour = 0
            minute = 0
            second = 0
        return datetime.datetime(year,
                                 month,
                                 day,
                                 hour,
                                 minute,
                                 second)


def trunc_opp_id(opp_id):
    if len(str(opp_id)) < 10:
        return np.nan
    else:
        return opp_id[0:15]


def get_dates_from_range(start_date, end_date, date_type):
    dates = []
    if date_type == 'Week':
        current_date = get_week_from_date(start_date)
        while current_date < end_date:
            dates.append(current_date)
            current_date = current_date + datetime.timedelta(days=7)
            current_date = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
    elif date_type == 'Month':
        current_date = get_month_from_date(start_date)
        while current_date < end_date:
            dates.append(current_date)
            current_date = current_date + relativedelta(months=1)
            current_date = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
    return dates


def get_month_from_date(input_date):
    try:
        month = input_date.replace(day=1)
        return month.replace(hour=0, minute=0, second=0, microsecond=0)
    except:
        return np.nan


def get_time_to_end_date(start_date, end_date):
    try:
        difference = end_date - start_date
        hours_to_end_date = difference / np.timedelta64(1, 'h')
        days_to_end_date = hours_to_end_date / 24

    except:
        days_to_end_date = np.nan
        hours_to_end_date = np.nan

    return {'Days': days_to_end_date, 'Hours': hours_to_end_date}

def check_is_inbound(source):
    try:
        if re.search('Inbound', source) is not None:
            return True
        else:
            return False
    except:
        return False

# Update date fields for Google Sheets
def convert_date_to_str(x):
    if pd.notnull(x):
        return x.strftime('%m/%d/%Y')
    else:
        return np.nan

def convert_datetime_to_str(x):
    if pd.notnull(x):
        # hack for server on different time zone
        x = x - datetime.timedelta(hours=5)
        return x.strftime("%m/%d/%Y, %H:%M:%S")
    else:
        return np.nan

def get_week_from_date(input_date):
    try:
        week = input_date - datetime.timedelta(days=input_date.weekday())
        return week.replace(hour=0, minute=0, second=0, microsecond=0)
    except:
        return np.nan


def get_data_over_time_all_sources(df, region, region_type, start_date, end_date):
    sources = ['Inbound', 'Outbound', 'Referral', 'Other']

    # make sure we are only considering kitchens
    weekly_data_df = get_data_by_date_range(df, region, region_type, 'All', start_date, end_date, 'Week')
    monthly_data_df = get_data_by_date_range(df, region, region_type, 'All', start_date, end_date, 'Month')
    for source in sources:
        source_df = df[df['Source'] == source].reset_index(drop=True)
        weekly_data_df = weekly_data_df.append(
            get_data_by_date_range(source_df, region, region_type, source, start_date, end_date, 'Week'))
        monthly_data_df = monthly_data_df.append(
            get_data_by_date_range(source_df, region, region_type, source, start_date, end_date, 'Month'))

    # Map AEs from 'All' sources to other sources
    new_weekly_df = weekly_data_df[weekly_data_df['Source'] == 'All'][
        ['Week', 'Region', 'RegionType', 'ActiveAEs', 'RampedAEs']].reset_index(drop=True)
    new_weekly_df.rename(columns={'Week': 'NewWeek', 'Region': 'NewRegion', 'RegionType': 'NewRegionType'},
                         inplace=True)
    weekly_data_df.drop(columns=['ActiveAEs', 'RampedAEs'], inplace=True)
    weekly_data_df = weekly_data_df.merge(new_weekly_df,
                                          left_on=['Week', 'Region', 'RegionType'],
                                          right_on=['NewWeek', 'NewRegion', 'NewRegionType'],
                                          how="left")
    weekly_data_df.drop(columns=['NewWeek', 'NewRegion', 'NewRegionType'], inplace=True)

    new_monthly_df = monthly_data_df[monthly_data_df['Source'] == 'All'][
        ['Month', 'Region', 'RegionType', 'ActiveAEs', 'RampedAEs']].reset_index(drop=True)
    new_monthly_df.rename(columns={'Month': 'NewMonth', 'Region': 'NewRegion', 'RegionType': 'NewRegionType'},
                          inplace=True)
    monthly_data_df.drop(columns=['ActiveAEs', 'RampedAEs'], inplace=True)
    monthly_data_df = monthly_data_df.merge(new_monthly_df,
                                            left_on=['Month', 'Region', 'RegionType'],
                                            right_on=['NewMonth', 'NewRegion', 'NewRegionType'],
                                            how="left")
    monthly_data_df.drop(columns=['NewMonth', 'NewRegion', 'NewRegionType'], inplace=True)

    return weekly_data_df, monthly_data_df


def get_data_by_date_range(df, region, region_type, source, start_date, end_date, date_type):
    dates = get_dates_from_range(start_date, end_date, date_type)
    columns = [date_type, 'Region', 'RegionType', 'Source', 'LeadsCreated', 'LeadsConverted', 'OppsCreated',
               'Pitched', 'Negotiated', 'Committed', 'ClosedWon', 'ClosedLost', 'Nurturing', 'Churned', 'StuckOpps',
               'ActiveAEs', 'RampedAEs']
    dates_df = pd.DataFrame(columns=columns)
    kitchens_df = df[df['Kitchen_Type__c'] == 'Delivery'].reset_index(drop=True)
    for date in dates:
        date_col = {columns[0]: date}
        region_col = {columns[1]: region}
        region_type_col = {columns[2]: region_type}
        source_col = {columns[3]: source}
        leads_created = {
            columns[4]: df[df['LeadCreatedDate' + date_type] == date]['LeadCreatedDate' + date_type].count(),
            columns[5]: df[(df['LeadCreatedDate' + date_type] == date) &
                           (df['QualifiedDate'].notnull())]['LeadCreatedDate' + date_type].count()}
        opps_created = {columns[6]: kitchens_df[kitchens_df['QualifiedDate' + date_type] == date][
            'QualifiedDate' + date_type].count()}
        opps_pitched = {columns[7]: kitchens_df[kitchens_df['PitchingDate' + date_type] == date][
            'PitchingDate' + date_type].count()}
        opps_negotiated = {
            columns[8]: kitchens_df[kitchens_df['NegotiationDate' + date_type] == date][
                'NegotiationDate' + date_type].count()}
        opps_committed = {
            columns[9]: kitchens_df[kitchens_df['CommitmentDate' + date_type] == date][
                'CommitmentDate' + date_type].count()}
        opps_closed_won = {
            columns[10]: kitchens_df[(kitchens_df['ClosedWonDate' + date_type] == date) &
                                     (kitchens_df['StageName'] == 'Closed Won')]['ClosedWonDate' + date_type].count()}
        opps_closed_lost = {
            columns[11]: kitchens_df[kitchens_df['ClosedLostDate' + date_type] == date][
                'ClosedLostDate' + date_type].count()}
        opps_nurturing = {
            columns[12]: kitchens_df[kitchens_df['NurturingDate' + date_type] == date][
                'NurturingDate' + date_type].count()}
        opps_churned = {
            columns[13]: kitchens_df[kitchens_df['ChurnDate' + date_type] == date]['ChurnDate' + date_type].count()}
        opps_stuck = {
            columns[14]: kitchens_df[(kitchens_df['LastActivityDate' + date_type] == date) &
                                     (kitchens_df['OpportunityId'].notnull()) &
                                     (kitchens_df['Closed_Won_Date__c'].isnull()) &
                                     (kitchens_df['Closed_Lost_Date__c'].isnull()) &
                                     (kitchens_df['Nurturing_Date__c'].isnull())][
                'LastActivityDate' + date_type].count()}
        total_aes = {
            columns[15]: kitchens_df[(kitchens_df['RoleName'] == 'Account Executive') & (
                    kitchens_df['QualifiedDate' + date_type] == date)][
                'UserId'].nunique()}
        ramped_aes = {columns[16]: kitchens_df[
            (kitchens_df['RoleName'] == 'Account Executive') & (kitchens_df['QualifiedDate' + date_type] == date) &
            kitchens_df['IsRamped']][
            'UserId'].nunique()}
        dates_data = {**date_col, **region_col, **region_type_col, **source_col, **leads_created,
                      **opps_created, **opps_pitched, **opps_negotiated, **opps_committed, **opps_closed_won,
                      **opps_closed_lost, **opps_nurturing, **opps_churned, **opps_stuck, **total_aes, **ramped_aes}
        dates_df = dates_df.append(pd.DataFrame(dates_data, index=[0]))
    return dates_df


def get_team_opp_data_by_date_range(df_all, name, user_city, user_country, user_region, role, source, user_start_date,
                                    user_end_date, is_active, start_date, end_date, date_type):
    dates = get_dates_from_range(start_date, end_date, date_type)

    columns = [date_type, 'Name', 'City', 'Country', 'Region', 'Role', 'Source', 'Start Date', 'End Date', 'Is Active',
               'OppsCreated', 'Pitched', 'Negotiated', 'Committed', 'ClosedWon', 'ClosedLost', 'Nurturing', 'Churned',
               'StuckOpps', 'OppsConverted', 'OppsPitchedConverted']
    dates_df = pd.DataFrame(columns=columns)
    df_all.loc[df_all['KitchenCity'].isnull(), ['KitchenCity']] = user_city
    df_all.loc[df_all['KitchenCountry'].isnull(), ['KitchenCity']] = user_country
    kitchen_cities = df_all[(df_all['KitchenCountry'] == user_country) & df_all['KitchenCity'].notnull()][[
        'KitchenCity', 'KitchenCountry']].drop_duplicates().reset_index(drop=True)
    for index, row in kitchen_cities.iterrows():
        df = df_all[(df_all['Kitchen_Type__c'] == 'Delivery') &
                    (df_all['KitchenCity'] == row['KitchenCity'])].reset_index(drop=True)

        for date in dates:
            date_col = {columns[0]: date}
            name_col = {columns[1]: name}
            city_col = {columns[2]: row['KitchenCity']}
            country_col = {columns[3]: user_country}
            region_col = {columns[4]: user_region}
            role_col = {columns[5]: role}
            source_col = {columns[6]: source}
            start_date_col = {columns[7]: user_start_date}
            end_date_col = {columns[8]: user_end_date}
            is_active_col = {columns[9]: is_active}
            opps_created = {columns[10]: df[df['QualifiedDate' + date_type] == date][
                'QualifiedDate' + date_type].count()}
            opps_pitched = {columns[11]: df[df['PitchingDate' + date_type] == date][
                'PitchingDate' + date_type].count()}
            opps_negotiated = {
                columns[12]: df[df['NegotiationDate' + date_type] == date][
                    'NegotiationDate' + date_type].count()}
            opps_committed = {
                columns[13]: df[df['CommitmentDate' + date_type] == date][
                    'CommitmentDate' + date_type].count()}
            opps_closed_won = {
                columns[14]: df[(df['ClosedWonDate' + date_type] == date) &
                                (df['StageName'] == 'Closed Won')][
                    'ClosedWonDate' + date_type].count()}
            opps_closed_lost = {
                columns[15]: df[df['ClosedLostDate' + date_type] == date][
                    'ClosedLostDate' + date_type].count()}
            opps_nurturing = {
                columns[16]: df[df['NurturingDate' + date_type] == date][
                    'NurturingDate' + date_type].count()}
            opps_churned = {
                columns[17]: df[df['ChurnDate' + date_type] == date]['ChurnDate' + date_type].count()}
            opps_stuck = {
                columns[18]: df[(df['LastActivityDate' + date_type] == date) &
                                (df['OpportunityId'].notnull()) &
                                (df['Closed_Won_Date__c'].isnull()) &
                                (df['Closed_Lost_Date__c'].isnull()) &
                                (df['Nurturing_Date__c'].isnull())][
                    'LastActivityDate' + date_type].count()}
            opps_converted = {
                columns[19]: df[(df['QualifiedDate' + date_type] == date) &
                                (df['StageName'] == 'Closed Won')][
                    'QualifiedDate' + date_type].count()}

            opps_pitched_converted = {
                columns[20]: df[(df['PitchingDate' + date_type] == date) &
                                (df['StageName'] == 'Closed Won')][
                    'PitchingDate' + date_type].count()}


            dates_data = {**date_col, **name_col, **city_col, **country_col, **region_col, **role_col,
                          **source_col, **start_date_col, **end_date_col, **is_active_col,
                          **opps_created, **opps_pitched, **opps_negotiated, **opps_committed,
                          **opps_closed_won,**opps_closed_lost, **opps_nurturing, **opps_churned, **opps_stuck,
                          **opps_converted, **opps_pitched_converted}

            sub_dates_data = {**opps_created, **opps_pitched, **opps_negotiated, **opps_committed,
                              **opps_closed_won, **opps_closed_lost, **opps_nurturing, **opps_churned, **opps_stuck,
                              **opps_converted, **opps_pitched_converted}

            count_vals = 0
            for d in sub_dates_data.items():
                try:
                    if d[1] > 0:
                        count_vals += 1
                except:
                    pass
            if count_vals > 0:
                dates_df = dates_df.append(pd.DataFrame({**dates_data}, index=[0]))
    return dates_df


def get_team_lead_data_by_date_range(df_all, name, user_city, user_country, user_region, role, source, user_start_date,
                                     user_end_date, is_active, start_date, end_date, date_type):
    dates = get_dates_from_range(start_date, end_date, date_type)
    columns = [date_type, 'Name', 'City', 'Country', 'Region', 'Role', 'Source', 'Start Date', 'End Date', 'Is Active',
               'LeadsCreated', 'LeadsConverted', 'LeadsConvertedPitching', 'OppsCreatedSDR']
    dates_df = pd.DataFrame(columns=columns)
    df_all.loc[df_all['KitchenCity'].isnull(), ['KitchenCity']] = user_city
    df_all.loc[df_all['KitchenCountry'].isnull(), ['KitchenCountry']] = user_city
    kitchen_cities = df_all[(df_all['KitchenCountry'] == user_country) & df_all['KitchenCity'].notnull()][
        ['KitchenCountry', 'KitchenCity']].drop_duplicates().reset_index(drop=True)
    for index, row in kitchen_cities.iterrows():
        df = df_all[df_all['KitchenCity'] == row['KitchenCity']].reset_index(drop=True)
        if len(df) == 0:
            continue
        for date in dates:
            date_col = {columns[0]: date}
            name_col = {columns[1]: name}
            city_col = {columns[2]: row['KitchenCity']}
            country_col = {columns[3]: user_country}
            region_col = {columns[4]: user_region}
            role_col = {columns[5]: role}
            source_col = {columns[6]: source}
            start_date_col = {columns[7]: user_start_date}
            end_date_col = {columns[8]: user_end_date}
            is_active_col = {columns[9]: is_active}
            leads_created = {
                columns[10]: df[df['LeadCreatedDate' + date_type] == date]['LeadCreatedDate' + date_type].count(),
                columns[11]: df[(df['LeadCreatedDate' + date_type] == date) &
                                (df['QualifiedDate'].notnull())]['LeadCreatedDate' + date_type].count(),
                columns[12]: df[(df['LeadCreatedDate' + date_type] == date) &
                                (df['Pitching_Date__c'].notnull())]['LeadCreatedDate' + date_type].count()
            }

            if role == 'Sales Development Representative':
                sdr_opps_created = df[(df['QualifiedDate' + date_type] == date) &
                                      (df['QualifiedDate'].notnull())]['QualifiedDate' + date_type].count()
            else:
                sdr_opps_created = 0

            sdr_opps = {columns[13]: sdr_opps_created}

            dates_data = {**date_col, **name_col, **city_col, **country_col, **region_col, **role_col,
                          **source_col, **start_date_col, **end_date_col, **is_active_col, **leads_created, **sdr_opps}
            sub_dates_data = {**leads_created, **sdr_opps}
            count_vals = 0
            for d in sub_dates_data.items():
                try:
                    if d[1] > 0:
                        count_vals += 1
                except:
                    pass
            if count_vals > 0:
                dates_df = dates_df.append(pd.DataFrame({**dates_data}, index=[0]))
    return dates_df


def get_days_difference(start_date, end_date):
    try:
        difference = end_date - start_date
        days_difference = difference.days
    except:
        days_difference = np.nan
    return days_difference


def get_days_since_activity(activity_date):
    date_format = "%Y-%m-%d"
    if len(str(activity_date)) < 10:
        return None
    else:
        days_since_activity = datetime.datetime.now() - datetime.datetime.strptime(activity_date, date_format)
        return days_since_activity.days


def get_last_activity(converted_date, lead_activity_date, opp_activity_date):
    if len(str(converted_date)) < 10:
        last_activity_date = lead_activity_date
    else:
        last_activity_date = opp_activity_date
    return last_activity_date


def extract_css_id(notes):
    try:
        if re.search('CSS ID', notes) is not None:
            css_id = re.search('\w{20}', notes).group()
        else:
            css_id = np.nan
    except:
        css_id = np.nan

    return css_id


def clean_lead_source(source):
    try:
        if re.search('Inbound', source) is not None:
            clean_source = 'Inbound'
        elif re.search('Outbound', source) is not None:
            clean_source = 'Outbound'
        elif re.search('LeadGenius', source) is not None:
            clean_source = 'Outbound'
        elif re.search('Data Science', source) is not None:
            clean_source = 'Outbound'
        elif re.search('Referral', source) is not None:
            clean_source = 'Referral'
        elif re.search('Existing Customer', source) is not None:
            clean_source = 'Existing Customer'
        else:
            clean_source = 'Other'
    except:
        clean_source = 'Other'
    return clean_source


def clean_region(title, city, country):
    if pd.notnull(country):
        if country == 'Mexico':
            final_region = 'Mexico'
        elif country == 'Brazil':
            if city == 'Sao Paulo':
                final_region = 'Sao Paulo'
            elif city == 'Rio de Janeiro':
                final_region = 'Rio de Janeiro'
            elif city == 'Belo Horizonte':
                final_region = 'Belo Horizonte'
            else:
                final_region = 'Brazil-X'
        elif country in ('Colombia', 'Costa Rica', 'Panama'):
            final_region = 'CCAC'
        elif country in ('Peru', 'Ecuador'):
            final_region = 'Andean'
        elif country in ('Chile'):
            final_region = 'South Cone'
        else:
            final_region = country
    elif pd.notnull(title):
        if re.search('Brazil', title) is not None:
            final_region = 'Brazil-X'
        elif re.search('Sao Paulo', title) is not None:
            final_region = 'Sao Paulo'
        elif re.search('Andean', title) is not None:
            final_region = 'Andean'
        elif re.search('CCAC', title) is not None:
            final_region = 'CCAC'
        elif re.search('South Cone', title) is not None:
            final_region = 'South Cone'
        elif re.search('Mexico', title) is not None:
            final_region = 'Mexico'
        else:
            final_region = country
    else:
        final_region = np.nan

    return final_region


def get_mega_region(user_region, kitchen_country):
    if pd.notnull(user_region):
        mega_region = user_region
    elif pd.notnull(kitchen_country):
        if kitchen_country in ('Mexico', 'Brazil', 'Costa Rica', 'Colombia', 'Peru', 'Chile'):
            mega_region = 'LATAM'
        elif kitchen_country in ('United States', 'Canada'):
            mega_region = 'US/CAN'
        elif kitchen_country in ('Indonesia', 'Singapore', 'Malaysia', 'Japan', 'Australia', 'Taiwan'):
            mega_region = 'APACx'
        elif kitchen_country in ('United Kingdom', 'UAE', 'Kuwait', 'Saudi Arabia', 'Spain', 'France'):
            mega_region = 'EMEA'
        else:
            mega_region = np.nan
    else:
        mega_region = np.nan

    return mega_region


def get_stage_data(total_df, inactive_df, start_date_col, end_date_col, days_col):
    stage_count = total_df[start_date_col].count()
    stage_stuck_count = inactive_df[start_date_col][
        inactive_df[end_date_col].isnull()].count()
    stage_avg_days_at_opp = total_df[days_col].mean()
    stage_median_days_at_opp = total_df[days_col].median()
    return [stage_count, stage_stuck_count, stage_avg_days_at_opp, stage_median_days_at_opp]


def get_close_data(df):
    closed_won_count = df['Closed_Won_Date__c'].count()
    closed_lost_count = df['Closed_Lost_Date__c'].count()
    nurturing_count = df['Nurturing_Date__c'].count()
    churned_count = df['Churn_Date__c'].count()
    closed_won_days = df['DaysToClosedWon'].mean()
    return [closed_won_count, closed_lost_count, nurturing_count, churned_count, closed_won_days]


def agg_data(df, region, region_type, kitchen_type, source, ramped_owner, time_frame):
    region = {'Region': region, 'Region Type': region_type}
    kitchen = {'KitchenType': kitchen_type}
    source = {'Source': source}
    ramped_owner = {'OwnerStatus': ramped_owner}
    time_frame = {'Time Frame': time_frame}
    inactive = df[df['DaysSinceActivity'] > 14].reset_index(drop=True)
    inactive_working_leads = df[(df['DaysSinceActivity'] > 14) &
                                (df['LeadStatus'] == 'Working')].reset_index(drop=True)
    total_leads = {'Total Leads': df['LeadId'].count()}
    worked_leads_data = get_stage_data(df, inactive_working_leads, 'WorkingDate', 'QualifiedDate',
                                       'DaysWorking')
    worked_leads = {'Worked Leads': worked_leads_data[0],
                    'Stuck Worked Leads': worked_leads_data[1],
                    'Avg Days at Working': worked_leads_data[2]}
    opps_data = get_stage_data(df, inactive, 'QualifiedDate', 'Pitching_Date__c',
                               'DaysOpportunity')
    opps = {'Opps': opps_data[0],
            'Stuck Opps': opps_data[1],
            'Avg Days at Opps': opps_data[2]}
    pitching_data = get_stage_data(df, inactive, 'Pitching_Date__c', 'Negotiation_Date__c',
                                   'DaysPitching')
    pitching = {'Pitching': pitching_data[0],
                'Stuck Pitching': pitching_data[1],
                'Avg Days at Pitching': pitching_data[2]}
    negotiating_data = get_stage_data(df, inactive, 'Negotiation_Date__c', 'Commitment_Date__c',
                                      'DaysNegotiating')
    negotiating = {'Negotiating': negotiating_data[0],
                   'Stuck Negotiating': negotiating_data[1],
                   'Avg Days at Negotiating': negotiating_data[2]}
    commitment_data = get_stage_data(df, inactive, 'Commitment_Date__c', 'Closed_Won_Date__c',
                                     'DaysCommitting')
    committing = {'Committing': commitment_data[0],
                  'Stuck Committing': commitment_data[1],
                  'Avg Days at Committing': commitment_data[2]}
    close_data = get_close_data(df)
    close = {'Closed Won': close_data[0],
             'Closed Lost': close_data[1],
             'Nurturing': close_data[2],
             'Churned': close_data[3],
             'Days To Closed Won': close_data[4]}
    final_dict = {**region, **kitchen, **source, **ramped_owner, **time_frame, **total_leads, **worked_leads, **opps,
                  **pitching, **negotiating, **committing, **close}

    # leads numbers are incorrect for owner status, so remove this number if not equal to 'All'
    final_df = pd.DataFrame(final_dict, index=[0])
    final_df.loc[final_df['OwnerStatus'] != 'All', ['Total Leads', 'Worked Leads', 'Stuck Worked Leads']] = np.nan
    final_df.loc[final_df['KitchenType'] != 'All', ['Total Leads', 'Worked Leads', 'Stuck Worked Leads']] = np.nan

    return final_df


def agg_data_by_source(df, region, region_type, kitchen_type, ramped_owner, time_frame):
    sources = ['Other', 'Outbound', 'Inbound', 'Referral']
    new_df = agg_data(df, region, region_type, kitchen_type, 'All', ramped_owner, time_frame)
    for source in sources:
        sub_df = df[df['Source'] == source].reset_index(drop=True)
        source_df = agg_data(sub_df, region, region_type, kitchen_type, source, ramped_owner, time_frame)
        new_df = new_df.append(source_df)
    return new_df


def agg_data_owner_status(df, region, region_type, kitchen_type, time_frame):
    final_df = agg_data_by_source(df, region, region_type, kitchen_type, 'All', time_frame)
    final_df = final_df.append(
        agg_data_by_source(df[df['IsRamped']].reset_index(drop=True), region, region_type, kitchen_type, 'Ramped',
                           time_frame))
    final_df = final_df.append(
        agg_data_by_source(df[df['IsRamping']].reset_index(drop=True), region, region_type, kitchen_type, 'Ramping',
                           time_frame))
    final_df = final_df.append(
        agg_data_by_source(df[df['IsOnboarding']].reset_index(drop=True), region, region_type, kitchen_type,
                           'Onboarding',
                           time_frame))
    final_df.reset_index(drop=True, inplace=True)
    return final_df


def agg_data_by_region(df, region, region_type, time_frame):
    final_df = agg_data_owner_status(df, region, region_type, 'All', time_frame)
    df_delivery = df[df['Kitchen_Type__c'] == 'Delivery'].reset_index(drop=True)
    final_df = final_df.append(agg_data_owner_status(df_delivery, region, region_type, 'Delivery', time_frame))
    df_virtual = df[df['Kitchen_Type__c'] == 'Virtual'].reset_index(drop=True)
    final_df = final_df.append(agg_data_owner_status(df_virtual, region, region_type, 'Virtual', time_frame))
    final_df.reset_index(drop=True, inplace=True)
    return final_df


def check_is_ramped(user_start_date, opportunity_created_date):
    if get_days_difference(user_start_date, opportunity_created_date) >= 90:
        return True
    else:
        return False


def check_is_onboarding(user_start_date, opportunity_created_date):
    if get_days_difference(user_start_date, opportunity_created_date) <= 30:
        return True
    else:
        return False


def check_is_ramping(user_start_date, opportunity_created_date):
    if (get_days_difference(user_start_date, opportunity_created_date) > 30) & (
            get_days_difference(user_start_date, opportunity_created_date) < 90):
        return True
    else:
        return False


def update_lead_scores(merged_data, csv_path):
    # merge leads data with scoring
    lead_scores = pd.read_csv(csv_path)
    merged_data = merged_data.merge(lead_scores[['css_parent_chain_uuid', 'tier', 'final_score']],
                                    left_on='CSSId',
                                    right_on='css_parent_chain_uuid',
                                    how='left')
    merged_data.drop(columns=['css_parent_chain_uuid'], inplace=True)
    merged_data.rename(columns={'tier': 'CSSTier', 'final_score': 'CSSScore'}, inplace=True)
    return merged_data


def get_activity_data(merged_data):
    # Get last activity date
    merged_data['LastActivityDate'] = merged_data[['QualifiedDate',
                                                   'LastLeadActivityDate',
                                                   'LastOppActivityDate']].apply(
        lambda x: get_last_activity(x[0], x[1], x[2]), axis=1)

    merged_data['LastActivityDate'] = merged_data['LastActivityDate'].apply(lambda x: convert_to_timestamp(x))

    # drop columns we don't need
    merged_data.drop(columns=['LastOppActivityDate', 'LastLeadActivityDate'], inplace=True)


def add_facility_data(merged_data, facility_df, col):
    new_data = merged_data[merged_data['FacilityId'].notnull()][['FacilityId', col, 'OpportunityId']].groupby(
        ['FacilityId', col]).count().reset_index()
    new_data = new_data.sort_values('OpportunityId', ascending=False).drop_duplicates(['FacilityId']).reset_index(
        drop=True)
    new_data.drop(columns=['OpportunityId'], inplace=True)
    facility_data = facility_df.merge(new_data, left_on="Id", right_on="FacilityId", how="left")
    facility_data.drop(columns=['FacilityId'], inplace=True)
    return facility_data


def add_facility_stage_data(merged_df, facility_df, col, stage_col, next_stage_col):
    new_df = merged_df[(merged_df['FacilityId'].notnull()) &
                       (merged_df[stage_col].notnull()) &
                       (merged_df[next_stage_col].isnull())][['FacilityId', 'OpportunityId']]
    new_df = new_df.groupby('FacilityId').count().reset_index()
    new_df.rename(columns={'OpportunityId': col}, inplace=True)
    facility_data = facility_df.merge(new_df, left_on="Id", right_on="FacilityId", how="left")
    facility_data.drop(columns=['FacilityId'], inplace=True)
    return facility_data


def add_facility_close_data(merged_df, facility_df):
    # Closed Won
    cw_df = merged_df[(merged_df['FacilityId'].notnull()) &
                      (merged_df['Closed_Won_Date__c'].notnull())][['FacilityId', 'OpportunityId']]
    cw_df = cw_df.groupby('FacilityId').count().reset_index()
    cw_df.rename(columns={'OpportunityId': 'FacilityClosedWon'}, inplace=True)
    facility_df = facility_df.merge(cw_df, left_on="Id", right_on="FacilityId", how="left")
    facility_df.drop(columns=['FacilityId'], inplace=True)

    # Closed Lost
    cl_df = merged_df[(merged_df['FacilityId'].notnull()) &
                      (merged_df['Closed_Lost_Date__c'].notnull())][['FacilityId', 'OpportunityId']]
    cl_df = cl_df.groupby('FacilityId').count().reset_index()
    cl_df.rename(columns={'OpportunityId': 'FacilityClosedLost'}, inplace=True)
    facility_df = facility_df.merge(cl_df, left_on="Id", right_on="FacilityId", how="left")
    facility_df.drop(columns=['FacilityId'], inplace=True)

    # Nurturing
    n_df = merged_df[(merged_df['FacilityId'].notnull()) &
                     (merged_df['Nurturing_Date__c'].notnull())][['FacilityId', 'OpportunityId']]
    n_df = n_df.groupby('FacilityId').count().reset_index()
    n_df.rename(columns={'OpportunityId': 'FacilityNurturing'}, inplace=True)
    facility_df = facility_df.merge(n_df, left_on="Id", right_on="FacilityId", how="left")
    facility_df.drop(columns=['FacilityId'], inplace=True)

    # Churned
    ch_df = merged_df[(merged_df['FacilityId'].notnull()) &
                      (merged_df['Churned_check_for_yes__c'])][['FacilityId', 'OpportunityId']]
    ch_df = ch_df.groupby('FacilityId').count().reset_index()
    ch_df.rename(columns={'OpportunityId': 'FacilityChurned'}, inplace=True)
    facility_df = facility_df.merge(ch_df, left_on="Id", right_on="FacilityId", how="left")
    facility_df.drop(columns=['FacilityId'], inplace=True)

    return facility_df


def get_data_by_facility(df):
    facility_data = df[df['FacilityId'].notnull()][['FacilityId']].groupby('FacilityId').count().reset_index()
    facility_data.rename(columns={'FacilityId': 'Id'}, inplace=True)
    facility_data = add_facility_data(df, facility_data, 'FacilityName')
    facility_data = add_facility_data(df, facility_data, 'KitchenCity')
    facility_data = add_facility_data(df, facility_data, 'KitchenCountry')
    facility_data = add_facility_data(df, facility_data, 'UserRegion')
    facility_data = add_facility_data(df, facility_data, 'SubRegion')
    facility_data = add_facility_data(df, facility_data, 'Go_Live_Date__c')
    facility_data = add_facility_data(df, facility_data, 'Live__c')
    facility_data.rename(columns={'UserRegion': 'Region'}, inplace=True)
    facility_data = add_facility_data(df, facility_data, 'Capacity__c')
    facility_data = add_facility_stage_data(df, facility_data, 'FacilityOpps', 'QualifiedDate', 'Pitching_Date__c')
    facility_data = add_facility_stage_data(df, facility_data, 'FacilityPitching', 'Pitching_Date__c',
                                            'Negotiation_Date__c')
    facility_data = add_facility_stage_data(df, facility_data, 'FacilityNegotiations', 'Negotiation_Date__c',
                                            'Commitment_Date__c')
    facility_data = add_facility_stage_data(df, facility_data, 'FacilityCommitments', 'Commitment_Date__c',
                                            'Closed_Won_Date__c')
    facility_data = add_facility_close_data(df, facility_data)
    facility_data = facility_data[(facility_data['Region'].notnull()) &
                                  (facility_data['Capacity__c'].notnull())].reset_index(drop=True)
    return facility_data


def add_date_group(merged_data, column, new_column):
    weekly_column = new_column + 'Week'
    monthly_column = new_column + 'Month'
    merged_data[weekly_column] = merged_data[column].apply(lambda x: get_week_from_date(x))
    merged_data[monthly_column] = merged_data[column].apply(lambda x: get_month_from_date(x))


def merge_facility_data(merged_data, facility_data):
    merged_data = merged_data.merge(
        facility_data[['FacilityId', 'FacilityName', 'Go_Live_Date__c', 'Capacity__c', 'Live__c']],
        left_on="Facility__c",
        right_on="FacilityId",
        how="left")
    merged_data.drop(columns=['Facility__c'], inplace=True)
    return merged_data


def map_user_data(global_leads_data, global_opps_data, global_user_data):
    sub_user_data = global_user_data[[
        'UserId',
        'UserName',
        'UserStartDate',
        'ProfileName',
        'RoleName',
        'Managers_Full_Name__c',
        'IsActive',
        'City',
        'Title',
        'Country',
        'UserSubRegion',
        'Region__c']].reset_index(drop=True)
    sub_user_data.rename(columns={
        'Region__c': 'UserRegion',
        'City': 'UserCity',
        'Country': 'UserCountry'
    }, inplace=True)

    pre_merged_data = global_leads_data.merge(global_opps_data, left_on="ConvertedAccountId",
                                              right_on="AccountId",
                                              how="outer")

    opps_merged_data = pre_merged_data[pre_merged_data['OpportunityId'].notnull()].merge(sub_user_data,
                                                                                         left_on="OppOwnerId",
                                                                                         right_on="UserId",
                                                                                         how="left")
    opps_merged_data = opps_merged_data.drop_duplicates(subset=['OpportunityId'],  keep='first', ignore_index=True)
    leads_merged_data = pre_merged_data[pre_merged_data['OpportunityId'].isnull()].merge(sub_user_data,
                                                                                         left_on="LeadOwnerId",
                                                                                         right_on="UserId",
                                                                                         how="left")
    leads_merged_data.drop_duplicates(subset=['LeadId'], inplace=True, keep='first', ignore_index=True)
    merged_data = opps_merged_data.append(leads_merged_data).reset_index(drop=True)

    merged_data.loc[merged_data['OpportunityId'].isnull(), ['Source']] = merged_data['LeadSource']
    merged_data.loc[merged_data['OpportunityId'].isnull(), ['Score']] = merged_data['LeadScore']
    merged_data.loc[merged_data['OpportunityId'].isnull(), ['KitchenCity']] = merged_data['LeadKitchenCity']
    merged_data.loc[merged_data['OpportunityId'].isnull(), ['KitchenCountry']] = merged_data['LeadKitchenCountry']
    merged_data.loc[merged_data['OpportunityId'].isnull(), ['Name']] = merged_data['LeadName']

    # Update lead dates for opps that don't have leads mapped to them. for tracking purposes
    #merged_data.loc[merged_data['LeadCreatedDate'].isnull(), ['LeadCreatedDate']] = merged_data['QualifiedDate']

    # extract CSS IDs for lead scoring (where available)
    merged_data['CSSId'] = merged_data['Notes__c'].apply(lambda x: extract_css_id(x))

    # update lead source and region based on opportunity and lead data
    merged_data['OriginalSource'] = merged_data['Source']
    merged_data['Source'] = merged_data['Source'].apply(
        lambda x: clean_lead_source(x))
    merged_data['SubRegion'] = merged_data[['Title', 'KitchenCity', 'KitchenCountry']].apply(
        lambda x: clean_region(x[0], x[1], x[2]), axis=1)
    merged_data['MegaRegion'] = merged_data[['UserRegion', 'KitchenCountry']].apply(
        lambda x: get_mega_region(x[0], x[1]), axis=1)

    # drop columns we don't need
    merged_data.drop(
        columns=['ConvertedOpportunityId',
                 'LeadScore',
                 'LeadKitchenCity',
                 'LeadKitchenCountry',
                 'LeadName',
                 'OppOwnerId'],
        inplace=True)

    # drop duplicates
    merged_data = merged_data[merged_data['LeadStatus'] != 'Duplicate'].reset_index(drop=True)

    # check if ae is ramped (90+ days), onboarding (<30 days), or ramping (30-90 days)
    merged_data['IsRamped'] = merged_data[['UserStartDate', 'QualifiedDate']].apply(
        lambda x: check_is_ramped(x[0], x[1]), axis=1)
    merged_data['IsOnboarding'] = merged_data[['UserStartDate', 'QualifiedDate']].apply(
        lambda x: check_is_onboarding(x[0], x[1]), axis=1)
    merged_data['IsRamping'] = merged_data[['UserStartDate', 'QualifiedDate']].apply(
        lambda x: check_is_ramping(x[0], x[1]), axis=1)

    return merged_data


def merge_sf_data(global_user_data, global_leads_data, global_opportunity_data, global_facility_data):
    # merge lead and opps dataframes, making sure that primary user data is opps data, unless no opp was created
    merged_data = map_user_data(global_leads_data, global_opportunity_data, global_user_data)
    merged_data = merge_facility_data(merged_data, global_facility_data)

    # add last activity dates and days since last activity
    get_activity_data(merged_data)

    # calculate the weeks and months tied to various stage dates for grouping purposes
    add_date_group(merged_data, 'LeadCreatedDate', 'LeadCreatedDate')
    add_date_group(merged_data, 'QualifiedDate', 'QualifiedDate')
    add_date_group(merged_data, 'Pitching_Date__c', 'PitchingDate')
    add_date_group(merged_data, 'Negotiation_Date__c', 'NegotiationDate')
    add_date_group(merged_data, 'Commitment_Date__c', 'CommitmentDate')
    add_date_group(merged_data, 'Closed_Won_Date__c', 'ClosedWonDate')
    add_date_group(merged_data, 'Closed_Lost_Date__c', 'ClosedLostDate')
    add_date_group(merged_data, 'Nurturing_Date__c', 'NurturingDate')
    add_date_group(merged_data, 'Churn_Date__c', 'ChurnDate')
    add_date_group(merged_data, 'LastActivityDate', 'LastActivityDate')

    # Drop any duplicates
    merged_data = merged_data[(merged_data['MegaRegion'] == 'US/CAN') |
                              (merged_data['MegaRegion'] == 'LATAM') |
                              (merged_data['MegaRegion'] == 'EMEA') |
                              (merged_data['MegaRegion'] == 'APACx')].reset_index(drop=True)
    merged_data.drop_duplicates(inplace=True)

    return merged_data


def get_funnel_data(global_data, recent_data, recent_timeframe):
    # Get aggregated funnel data - all time and h1 2020
    final_data = agg_data_by_region(global_data, 'Global', 'Mega-Region', 'All Time')
    final_data = final_data.append(agg_data_by_region(recent_data, 'Global', 'Mega-Region', recent_timeframe))

    print("Pulling weekly and Monthly Data...")
    regions = ['US/CAN', 'APACx', 'EMEA', 'LATAM']
    for region in regions:
        region_data = global_data[global_data['MegaRegion'] == region]
        final_data = final_data.append(agg_data_by_region(region_data, region, 'Mega-Region', 'All Time'))
        region_data_recent = recent_data[recent_data['MegaRegion'] == region]
        final_data = final_data.append(
            agg_data_by_region(region_data_recent, region, 'Mega-Region', recent_timeframe))

    latam_regions = ['Andean', 'CCAC', 'Brazil-X', 'Mexico', 'Sao Paulo', 'South Cone']
    for region in latam_regions:
        region_data = global_data[global_data['SubRegion'] == region]
        final_data = final_data.append(agg_data_by_region(region_data, region, 'Sub-Region', 'All Time'))
        region_data_recent = recent_data[recent_data['SubRegion'] == region]
        final_data = final_data.append(
            agg_data_by_region(region_data_recent, region, 'Sub-Region', recent_timeframe))

    latam_cities = list(global_data[global_data['UserRegion'] == 'LATAM']['KitchenCity'].unique())
    for city in latam_cities:
        city_data = global_data[global_data['KitchenCity'] == city]
        final_data = final_data.append(agg_data_by_region(city_data, city, 'City', 'All Time'))
        city_data_recent = recent_data[recent_data['KitchenCity'] == city]
        final_data = final_data.append(agg_data_by_region(city_data_recent, city, 'City', '2020'))

    return final_data


def get_time_interval_data(global_data, start_date, end_date):
    weekly_data, monthly_data = get_data_over_time_all_sources(global_data, 'Global', 'Mega-Region',
                                                               start_date,
                                                               end_date)
    regions = ['US/CAN', 'APACx', 'EMEA', 'LATAM']
    for region in regions:
        region_data = global_data[global_data['MegaRegion'] == region].reset_index(drop=True)
        region_weekly_data, region_monthly_data = get_data_over_time_all_sources(region_data,
                                                                                 region,
                                                                                 'Mega-Region',
                                                                                 start_date,
                                                                                 end_date)
        weekly_data = weekly_data.append(region_weekly_data)
        monthly_data = monthly_data.append(region_monthly_data)

    latam_regions = ['Andean', 'CCAC', 'Brazil-X', 'Mexico', 'Sao Paulo', 'South Cone']
    # Pull time data for latam sub-regions
    for region in latam_regions:
        region_data = global_data[global_data['SubRegion'] == region].reset_index(drop=True)
        region_weekly_data, region_monthly_data = get_data_over_time_all_sources(region_data,
                                                                                 region,
                                                                                 'Sub-Region',
                                                                                 start_date,
                                                                                 end_date)
        weekly_data = weekly_data.append(region_weekly_data)
        monthly_data = monthly_data.append(region_monthly_data)

    latam_cities = ['Monterrey', 'Guadalajara', 'Mexico City', 'Sao Paulo', 'Belo Horizonte', 'Rio de Janeiro',
                    'Bogota', 'Cali', 'Santiago']
    for city in latam_cities:
        city_data = global_data[global_data['KitchenCity'] == city]
        city_weekly_data, city_monthly_data = get_data_over_time_all_sources(city_data,
                                                                             city,
                                                                             'City',
                                                                             start_date,
                                                                             end_date)
        weekly_data = weekly_data.append(city_weekly_data)
        monthly_data = monthly_data.append(city_monthly_data)

    return weekly_data, monthly_data


def get_team_data_by_source(df, type, name, city, country, region, role, user_start_date, user_end_date, is_active,
                            start_date, end_date):
    sources = ['Inbound', 'Outbound', 'Referral', 'Other']
    weekly_data_df = pd.DataFrame()
    monthly_data_df = pd.DataFrame()

    for source in sources:
        source_df = df[df['Source'] == source].reset_index(drop=True)
        if type == 'Opps':
            weekly_data_df = weekly_data_df.append(
                get_team_opp_data_by_date_range(source_df, name, city, country, region, role, source, user_start_date,
                                                user_end_date, is_active, start_date, end_date, 'Week'))

            monthly_data_df = monthly_data_df.append(
                get_team_opp_data_by_date_range(source_df, name, city, country, region, role, source, user_start_date,
                                                user_end_date, is_active, start_date, end_date, 'Month'))
        else:
            weekly_data_df = weekly_data_df.append(
                get_team_lead_data_by_date_range(source_df, name, city, country, region, role, source, user_start_date,
                                                 user_end_date, is_active, start_date, end_date, 'Week'))

            monthly_data_df = monthly_data_df.append(
                get_team_lead_data_by_date_range(source_df, name, city, country, region, role, source, user_start_date,
                                                 user_end_date, is_active, start_date, end_date, 'Month'))
    weekly_data_df['End Date'].fillna('', inplace=True)
    weekly_data_df.fillna(0, inplace=True)
    monthly_data_df['End Date'].fillna('', inplace=True)
    monthly_data_df.fillna(0, inplace=True)
    return weekly_data_df, monthly_data_df


def get_team_data(global_data, global_user_data, user_ids, start_date, end_date):
    # initialize data frames
    team_weekly_all = pd.DataFrame()
    team_monthly_all = pd.DataFrame()

    print(len(user_ids), "to run...")
    count_runs = 0
    # Pull time data for latam aes
    for id in user_ids:
        user_opp_data = global_data[global_data['UserId'] == id].reset_index(drop=True)
        user_lead_data = global_data[global_data['LeadOwnerId'] == id].reset_index(drop=True)
        role = global_user_data[global_user_data['UserId'] == id]['RoleName'].values[0]
        city = global_user_data[global_user_data['UserId'] == id]['City'].values[0]
        country = global_user_data[global_user_data['UserId'] == id]['Country'].values[0]
        region = global_user_data[global_user_data['UserId'] == id]['UserSubRegion'].values[0]
        is_active = user_opp_data[user_opp_data['UserId'] == id]['IsActive'].values[0]
        name = global_user_data[global_user_data['UserId'] == id]['UserName'].values[0]
        user_start_date = global_user_data[global_user_data['UserId'] == id]['UserStartDate'].values[0]
        if role == 'Account Executive':
            user_end_date = global_user_data[global_user_data['UserId'] == id]['AE_End_Date__c'].values[0]
        elif role == 'Sales Development Representative':
            user_end_date = global_user_data[global_user_data['UserId'] == id]['SDR_End_Date__c'].values[0]
        else:
            user_end_date = np.nan

        user_opp_weekly_data, user_opp_monthly_data = get_team_data_by_source(user_opp_data,
                                                                              'Opps',
                                                                              name,
                                                                              city,
                                                                              country,
                                                                              region,
                                                                              role,
                                                                              user_start_date,
                                                                              user_end_date,
                                                                              is_active,
                                                                              start_date,
                                                                              end_date)

        user_lead_weekly_data, user_lead_monthly_data = get_team_data_by_source(user_lead_data,
                                                                                'Leads',
                                                                                name,
                                                                                city,
                                                                                country,
                                                                                region,
                                                                                role,
                                                                                user_start_date,
                                                                                user_end_date,
                                                                                is_active,
                                                                                start_date,
                                                                                end_date)

        user_weekly_data = user_opp_weekly_data.merge(user_lead_weekly_data,
                                                      left_on=['Week', 'Name', 'City', 'Country', 'Region', 'Role',
                                                               'Source', 'Start Date', 'End Date', 'Is Active'],
                                                      right_on=['Week', 'Name', 'City', 'Country', 'Region', 'Role',
                                                                'Source', 'Start Date', 'End Date', 'Is Active'],
                                                      how="outer").fillna(0)
        user_monthly_data = user_opp_monthly_data.merge(user_lead_monthly_data,
                                                        left_on=['Month', 'Name', 'City', 'Country', 'Region', 'Role',
                                                                 'Source', 'Start Date', 'End Date', 'Is Active'],
                                                        right_on=['Month', 'Name', 'City', 'Country', 'Region', 'Role',
                                                                  'Source', 'Start Date', 'End Date', 'Is Active'],
                                                        how="outer").fillna(0)

        team_weekly_all = team_weekly_all.append(user_weekly_data)
        team_weekly_all['id'] = id
        team_monthly_all = team_monthly_all.append(user_monthly_data)
        team_monthly_all['id'] = id

        count_runs += 1
        print(count_runs, "/", len(user_ids), "complete")

    # map onboarding status by date
    team_weekly_all.reset_index(drop=True, inplace=True)
    team_weekly_all['OnboardingStatus'] = ''
    for index, row in team_weekly_all.iterrows():
        # First check if ramping, ramped, or onboarding
        if get_days_difference(row['Start Date'], row['Week']) < 0:
            team_weekly_all.loc[index, 'OnboardingStatus'] = 'Not Started'
        elif get_days_difference(row['Start Date'], row['Week']) < 30:
            team_weekly_all.loc[index, 'OnboardingStatus'] = 'Onboarding'
        elif get_days_difference(row['Start Date'], row['Week']) < 90:
            team_weekly_all.loc[index, 'OnboardingStatus'] = 'Ramping'
        else:
            team_weekly_all.loc[index, 'OnboardingStatus'] = 'Ramped'

        # Then check if fired
        if get_days_difference(row['End Date'], row['Week']) > 0:
            team_weekly_all.loc[index, 'OnboardingStatus'] = 'Terminated'
        elif pd.isnull(row['End Date']) and row['Is Active'] == False:
            team_weekly_all.loc[index, 'OnboardingStatus'] = 'Terminated'

        # add activities

    team_monthly_all.reset_index(drop=True, inplace=True)
    team_monthly_all['OnboardingStatus'] = ''
    for index, row in team_monthly_all.iterrows():
        # First check if ramping, ramped, or onboarding
        get_days_difference(row['Start Date'], row['Month'])
        if get_days_difference(row['Start Date'], row['Month']) < 0:
            team_monthly_all.loc[index, 'OnboardingStatus'] = 'Not Started'
        elif get_days_difference(row['Start Date'], row['Month']) < 30:
            team_monthly_all.loc[index, 'OnboardingStatus'] = 'Onboarding'
        elif get_days_difference(row['Start Date'], row['Month']) < 90:
            team_monthly_all.loc[index, 'OnboardingStatus'] = 'Ramping'
        else:
            team_monthly_all.loc[index, 'OnboardingStatus'] = 'Ramped'

        # Then check if fired
        if get_days_difference(row['End Date'], row['Month']) > 0:
            team_monthly_all.loc[index, 'OnboardingStatus'] = 'Terminated'
        elif pd.isnull(row['End Date']) and row['Is Active'] == False:
            team_monthly_all.loc[index, 'OnboardingStatus'] = 'Terminated'

    # drop rows we don't need (when user had not started)
    # team_weekly_all = team_weekly_all[team_weekly_all['OnboardingStatus'] != 'Not Started'].reset_index(drop=True)
    # team_monthly_all = team_monthly_all[team_monthly_all['OnboardingStatus'] != 'Not Started'].reset_index(drop=True)

    return team_weekly_all, team_monthly_all


def first_non_null(first_n, second):
    if pd.isna(first_n):
        return second
    else:
        return first_n

def clean_lead_source_detail(x):
    gs = ['gsearch', 'gdisplay', 'gyoutube']
    string = ''
    try:
        if re.search(r'\??utm_source=(\w+)', x).group(1).lower() not in gs:
            string += re.search(r'(utm_source=[\w-]+&?)', x).group(1)
            string += re.search(r'(utm_channel=[\w-]+&?)', x).group(1)
            string += re.search(r'(utm_campaign=[\w-]+&?)', x).group(1)
            string += re.search(r'(utm_ad=[\w-]+)', x).group(1)
            return string.rstrip('&')
        elif re.search(r'\??utm_source=(\w+)', x).group(1).lower() in gs:
            return re.search(r'.+utm_campaign=([\w-]+)', x).group(1)
    except:
        return np.nan

