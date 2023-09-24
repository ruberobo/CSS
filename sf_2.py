
import json
import pandas as pd
from simple_salesforce import Salesforce, SalesforceLogin
import datetime
import helpers
import numpy as np

pd.options.mode.chained_assignment = None


class SF():

    def iter_to_df(self, iterator, fields):
        data = []
        for row in iterator:
            row_list = [row[x] for x in fields]
            data.append(row_list)

        dataframe = pd.DataFrame(data, columns=fields)

        return dataframe

    def iter_to_df_activities(self, iterator):
        fields = ['Id', 'OwnerId', 'Name']
        columns = fields + ['Type', 'Date','ActivityOwnerId']
        #
        data = []
        for row in iterator:
            try:
                activities = row['ActivityHistories']['records']
                for activity in activities:
                    row_list_main = [row[x] for x in fields]
                    type = activity['ActivityType']
                    date = activity['ActivityDate']
                    acowner = activity['OwnerId']
                    activities_list = [type, date, acowner]
                    row_list_main = row_list_main + activities_list
                    data.append(row_list_main)
            except:
                pass
        dataframe = pd.DataFrame(data, columns=columns)
        return dataframe

    def get_object_metadata(self, sfobject):
        metadata = sfobject.describe()
        df_metadata = pd.DataFrame(metadata.get('fields'))
        return df_metadata

    def get_lookup_value(self, source):
        fields = ['Id', 'Name']
        fields_txt = ' ,'.join(fields)
        query_text = f"SELECT {fields_txt} FROM {source}"
        iterator = self.sf.query_all_iter(query_text)
        df = self.iter_to_df(iterator, fields)
        return df

    def get_lead_data(self, region):
        fields = ['Id', 'Name', 'Company', 'Email', 'Phone', 'Website', 'LeadSource', 'Status', 'OwnerId',
                  'ConvertedOpportunityId', 'CreatedDate', 'LastActivityDate',
                  'Notes__c', 'Kitchen_City__c', 'Kitchen_Country__c', 'Lead_Score__c', 'Lead_Source_Detail__c',
                  'Qualified_Date__c', 'Unqualified_Reason__c', 'Estimated_Weekly_Orders__c', 'Refreshed_Lead__c',
                  'Number_of_Locations__c', 'CCS_ID__c', 'ConvertedAccountId', 'Lead_Closed_Reason__c',
                  'Kitchen_Type__c', 'Attended_Webinar__c', 'Did_Not_Attend_Webinar__c', 'Webinar_Topic__c',
                  'RecordTypeId','UTM_Campaign__c','UTM_Content__c','UTM_Medium__c','UTM_Source__c','UTM_Term__c',
          'utm_geo__c', 'UTM_Ad__c', 'UTM_Channel__c']
        fields_txt = ', '.join(fields)
        query_text = f"SELECT {fields_txt} FROM Lead WHERE OwnerId in (" \
                     f"SELECT Id FROM User WHERE Region__c in( 'LATAM','US/CAN')) " \
                     f"and (recordtypeid = '012f4000000Su90AAC' OR kitchen_type__c in ('Virtual', 'CloudRetail','virtual;cloudretail'))"

        lead_data_iterator = self.sf.query_all_iter(query_text)
        lead_df = self.iter_to_df(lead_data_iterator, fields)
        fields = ['Id', 'Name']
        query_text = "select id, name from RecordType"
        record_type_iterator = self.sf.query_all_iter(query_text)
        record_type_df = self.iter_to_df(record_type_iterator, fields)
        record_type_df.rename(columns={'Name': 'RecordTypeName',
                                       'Id': 'RecordTypeId'}, inplace=True)

        lead_df = lead_df.merge(record_type_df, on='RecordTypeId',  how='left')
        lead_df.rename(columns={'Id': 'LeadId',
                                'Name': 'LeadName',
                                'OwnerId': 'LeadOwnerId',
                                'LastActivityDate': 'LastLeadActivityDate',
                                'Lead_Score__c': 'LeadScore',
                                'Kitchen_City__c': 'LeadKitchenCity',
                                'Kitchen_Country__c': 'LeadKitchenCountry',
                                'CreatedDate': 'LeadCreatedDate',
                                'Kitchen_Type__c': 'LeadKitchenType',
                                'Status': 'LeadStatus'}, inplace=True)
        lead_df['LeadCreatedDate'] = lead_df['LeadCreatedDate'].apply(lambda x: helpers.convert_to_timestamp(x))
        return lead_df

    def get_lead_data_from_owner_id(self, owner_id):
        fields = ['Id', 'Name', 'Company', 'Email', 'Phone', 'Website', 'LeadSource', 'Status', 'OwnerId',
                  'ConvertedOpportunityId', 'ConvertedAccountId', 'CreatedDate', 'LastActivityDate',
                  'Notes__c', 'Kitchen_City__c', 'Kitchen_Country__c', 'Lead_Score__c', 'Lead_Source_Detail__c',
                  'Qualified_Date__c', 'Unqualified_Reason__c'
                  ,'UTM_Campaign__c','UTM_Content__c','UTM_Medium__c','UTM_Source__c','UTM_Term__c',
          'utm_geo__c']
        fields_txt = ', '.join(fields)
        query_text = f"SELECT {fields_txt} FROM Lead WHERE OwnerId = '{owner_id}'"
        lead_data_iterator = self.sf.query_all_iter(query_text)
        lead_df = self.iter_to_df(lead_data_iterator, fields)
        lead_df.rename(columns={'Id': 'LeadId',
                                'Name': 'LeadName',
                                'OwnerId': 'LeadOwnerId',
                                'LastActivityDate': 'LastLeadActivityDate',
                                'Lead_Score__c': 'LeadScore',
                                'Kitchen_City__c': 'LeadKitchenCity',
                                'Kitchen_Country__c': 'LeadKitchenCountry',
                                'CreatedDate': 'LeadCreatedDate',
                                'Status': 'LeadStatus'}, inplace=True)
        lead_df['LeadCreatedDate'] = lead_df['LeadCreatedDate'].apply(lambda x: helpers.convert_to_timestamp(x))
        return lead_df

    def get_account_data(self):
        fields = ['Id', 'Name', 'OwnerId', 'Phone', 'Website', 'LastActivityDate', 'Type']
        fields_txt = ', '.join(fields)
        query_text = f"SELECT {fields_txt} FROM Lead"
        account_data_iterator = self.sf.query_all_iter(query_text)
        account_df = self.iter_to_df(account_data_iterator, fields)
        account_df.rename(columns={'Id': 'AccountId',
                                   'OwnerId': 'AccountOwnerId',
                                   'LastActivityDate': 'LastAccountActivityDate'}, inplace=True)
        return account_df

    def get_activity_by_opportunity_id(self, opportunity_id):
        query_text = f"Select Id, OwnerId, Name, (Select Subject, ActivityDate, ActivityType from ActivityHistories) from Opportunity WHERE Id = '{opportunity_id}'"
        activity_hist_data_iterator = self.sf.query_all_iter(query_text)
        activity_hist_df = self.iter_to_df_activities(activity_hist_data_iterator)
        activity_hist_df.rename(columns={'Id': 'AccountId'}, inplace=True)
        return activity_hist_df

    def get_activity_by_owner_ids(self, owner_ids):
        owner_id_list = "', '".join(owner_ids)
        query_text = f"Select Id, OwnerId, Name, (Select Subject, ActivityDate, ActivityType, OwnerId from ActivityHistories) from Account WHERE OwnerId in ('{owner_id_list}')"
        activity_hist_data_iterator = self.sf.query_all_iter(query_text)
        activity_hist_df = self.iter_to_df_activities(activity_hist_data_iterator)
        activity_hist_df.rename(columns={'Id': 'AccountId'}, inplace=True)

        # convert date and add date week and month
        activity_hist_df['Date'] = activity_hist_df['Date'].apply(lambda x: helpers.convert_to_timestamp(x))
        activity_hist_df['Week'] = activity_hist_df['Date'].apply(lambda x: helpers.get_week_from_date(x))
        activity_hist_df['Month'] = activity_hist_df['Date'].apply(lambda x: helpers.get_month_from_date(x))
        return activity_hist_df

    def get_facility_tours_by_owner_ids(self, owner_ids):
        owner_id_list = "', '".join(owner_ids)
        query_text = f"Select Id, OwnerId, Name, (Select Subject, ActivityDate, ActivityType from ActivityHistories WHERE ActivityType = 'Facility Tour') from Account WHERE OwnerId in ('{owner_id_list}')"
        activity_hist_data_iterator = self.sf.query_all_iter(query_text)
        activity_hist_df = self.iter_to_df_activities(activity_hist_data_iterator)
        activity_hist_df.rename(columns={'Id': 'AccountId'}, inplace=True)

        # convert date and add date week and month
        activity_hist_df['Date'] = activity_hist_df['Date'].apply(lambda x: helpers.convert_to_timestamp(x))
        activity_hist_df['Week'] = activity_hist_df['Date'].apply(lambda x: helpers.get_week_from_date(x))
        activity_hist_df['Month'] = activity_hist_df['Date'].apply(lambda x: helpers.get_month_from_date(x))
        return activity_hist_df

    def get_region_lead_history(self, region):
        fields = ['LeadId',  'CreatedDate', 'Field', 'OldValue', 'NewValue']
        fields_txt = ', '.join(fields)
        query_text = f"SELECT {fields_txt} FROM LeadHistory " \
                     f"WHERE CreatedById in (SELECT Id FROM User WHERE Region__c in ('LATAM','US/CAN') or " \
                     f"id = '005f4000005tntGAAQ')  " \
                     f"and CreatedDate = LAST_N_DAYS:150 and Field = 'Status'  "
        lead_hist_data_iterator = self.sf.query_all_iter(query_text)
        lead_hist_df = self.iter_to_df(lead_hist_data_iterator, fields)
        # lead_hist_df.rename(columns={'Id': 'LeadHistoryId'}, inplace=True)
        return lead_hist_df

    def get_lead_history_by_id(self, lead_id):
        fields = ['Id', 'IsDeleted', 'LeadId', 'CreatedById', 'CreatedDate', 'Field', 'OldValue', 'NewValue']
        fields_txt = ', '.join(fields)
        query_text = f"SELECT {fields_txt} FROM LeadHistory WHERE LeadId = '{lead_id}'"
        lead_hist_data_iterator = self.sf.query_all_iter(query_text)
        lead_hist_df = self.iter_to_df(lead_hist_data_iterator, fields)
        lead_hist_df.rename(columns={'Id': 'LeadHistoryId'}, inplace=True)
        return lead_hist_df

    def get_region_lead_status_changes(self, region):
        print("Getting Status Changes...")
        lead_history = self.get_region_lead_history(region)
        lead_working_dates = lead_history[(lead_history['Field'] == 'Status') &
                                          (lead_history['NewValue'] == 'Working')][['LeadId', 'CreatedDate']].groupby(
            'LeadId').max().reset_index()
        lead_working_dates.rename(columns={'CreatedDate': 'WorkingDate'}, inplace=True)
        lead_processed_dates = lead_history[(lead_history['Field'] == 'Status') &
                                            (lead_history['NewValue'] != 'Working')][['LeadId', 'CreatedDate']].groupby(
            'LeadId').max().reset_index()
        lead_processed_dates.rename(columns={'CreatedDate': 'ProcessedDate'}, inplace=True)

        lead_status_dates = lead_working_dates.merge(lead_processed_dates, left_on="LeadId", right_on="LeadId",
                                                     how="outer")
        # convert to timestamps
        lead_status_dates['WorkingDate'] = lead_status_dates['WorkingDate'].apply(
            lambda x: helpers.convert_to_timestamp(x))
        lead_status_dates['ProcessedDate'] = lead_status_dates['ProcessedDate'].apply(
            lambda x: helpers.convert_to_timestamp(x))

        print("Status Changes Pulled...")

        return lead_status_dates

    def get_contact_history(self):
        fields = ['Id', 'IsDeleted', 'ContactId', 'CreatedById', 'CreatedDate', 'Field', 'OldValue', 'NewValue']
        fields_txt = ', '.join(fields)
        query_text = f"SELECT {fields_txt} FROM ContactHistory"
        contact_hist_data_iterator = self.sf.query_all_iter(query_text)
        contact_hist_df = self.iter_to_df(contact_hist_data_iterator, fields)
        contact_hist_df.rename(columns={'Id': 'ContactHistoryId'}, inplace=True)
        return contact_hist_df

    def get_contact_history_by_id(self, contact_id):
        fields = ['Id', 'IsDeleted', 'ContactId', 'CreatedById', 'CreatedDate', 'Field', 'OldValue', 'NewValue']
        fields_txt = ', '.join(fields)
        query_text = f"SELECT {fields_txt} FROM ContactHistory WHERE ContactId = '{contact_id}'"
        contact_hist_data_iterator = self.sf.query_all_iter(query_text)
        contact_hist_df = self.iter_to_df(contact_hist_data_iterator, fields)
        contact_hist_df.rename(columns={'Id': 'ContactHistoryId'}, inplace=True)
        return contact_hist_df

    def get_account_history(self):
        fields = ['Id', 'IsDeleted', 'AccountId', 'CreatedById', 'CreatedDate', 'Field', 'OldValue', 'NewValue']
        fields_txt = ', '.join(fields)
        query_text = f"SELECT {fields_txt} FROM AccountHistory"
        account_hist_data_iterator = self.sf.query_all_iter(query_text)
        account_hist_df = self.iter_to_df(account_hist_data_iterator, fields)
        account_hist_df.rename(columns={'Id': 'AccountHistoryId'}, inplace=True)
        return account_hist_df

    def get_account_history_by_id(self, account_id):
        fields = ['Id', 'IsDeleted', 'AccountId', 'CreatedById', 'CreatedDate', 'Field', 'OldValue', 'NewValue']
        fields_txt = ', '.join(fields)
        query_text = f"SELECT {fields_txt} FROM AccountHistory WHERE AccountId = '{account_id}'"
        account_hist_data_iterator = self.sf.query_all_iter(query_text)
        account_hist_df = self.iter_to_df(account_hist_data_iterator, fields)
        account_hist_df.rename(columns={'Id': 'AccountHistoryId'}, inplace=True)
        return account_hist_df

    def get_opportunity_history(self):
        fields = ['Id', 'IsDeleted', 'OpportunityId', 'CreatedById', 'CreatedDate', 'Field', 'OldValue', 'NewValue']
        fields_txt = ', '.join(fields)
        query_text = f"SELECT {fields_txt} FROM OpportunityHistory"
        opp_hist_data_iterator = self.sf.query_all_iter(query_text)
        opp_hist_df = self.iter_to_df(opp_hist_data_iterator, fields)
        opp_hist_df.rename(columns={'Id': 'OpportunityHistoryId'}, inplace=True)
        return opp_hist_df

    def get_opportunity_history_by_id(self, opportunity_id):
        fields = ['Id', 'OpportunityId', 'CreatedById', 'CreatedDate', 'StageName', 'Amount',
                  'ExpectedRevenue', 'CloseDate', 'Probability', 'ForecastCategory', 'CurrencyIsoCode',
                  'SystemModstamp', 'IsDeleted']
        fields_txt = ', '.join(fields)
        query_text = f"SELECT {fields_txt} FROM OpportunityHistory WHERE OpportunityId = '{opportunity_id}'"
        opp_hist_data_iterator = self.sf.query_all_iter(query_text)
        opp_hist_df = self.iter_to_df(opp_hist_data_iterator, fields)
        opp_hist_df.rename(columns={'Id': 'OpportunityHistoryId'}, inplace=True)
        return opp_hist_df

    def get_working_date(self, leads_data):
        lead_hist_df = self.get_lead_history()
        lead_hist_df = lead_hist_df[(lead_hist_df['NewValue'] == 'Working') &
                                    (lead_hist_df['Field'] == 'Status')][['LeadId', 'CreatedDate']].reset_index(
            drop=True)
        lead_hist_df.rename(columns={'LeadId': 'LeadIdHist', 'CreatedDate': 'WorkingDate'}, inplace=True)
        lead_hist_df.drop_duplicates(['LeadIdHist'], inplace=True)
        lead_hist_df['WorkingDate'] = lead_hist_df['WorkingDate'].apply(lambda x: helpers.convert_to_timestamp(x))
        leads_data = leads_data.merge(lead_hist_df, left_on='LeadId', right_on='LeadIdHist', how='left')
        # leads_data.loc[(leads_data['WorkingDate'].isnull() &
        #                (leads_data['LeadStatus'] != 'New')), ['WorkingDate']] = leads_data['LeadCreatedDate']
        return leads_data

    def get_event_data_by_owner(self, fields, owner_id):
        fields_txt = ', '.join(fields)
        query_text = f"SELECT {fields_txt} FROM Event WHERE OwnerId = '{owner_id}'"
        event_data_iterator = self.sf.query_all_iter(query_text)
        event_df = self.iter_to_df(event_data_iterator, fields)
        event_df.rename(columns={'Id': 'EventId'}, inplace=True)
        return event_df

    def get_event_data_by_lead(self, fields, lead_id):
        fields_txt = ', '.join(fields)
        query_text = f"SELECT {fields_txt} FROM Event WHERE WhoId = '{lead_id}'"
        event_data_iterator = self.sf.query_all_iter(query_text)
        event_df = self.iter_to_df(event_data_iterator, fields)
        event_df.rename(columns={'Id': 'EventId'}, inplace=True)
        return event_df

    def get_facility_data(self):
        fields = ['Id', 'Name', 'Region__c', 'Number_of_Kitchen_Instances__c',
                  'Total_Facility_Opportunities_Open__c', 'Total_Number_of_Opps__c', 'Site_Count__c', 'Go_Live_Date__c',
                  'Total_Facility_Opportunities_Won__c', 'Capacity_Percent__c', 'Capacity__c', 'Live__c',
                  'All_Opps_Closed_Lost__c', 'Country__c']
        fields_txt = ', '.join(fields)
        query_text = f"SELECT {fields_txt} FROM Account WHERE RecordTypeId = '012f4000000RcZ2AAK'"
        facility_data_iterator = self.sf.query_all_iter(query_text)
        facility_df = self.iter_to_df(facility_data_iterator, fields)
        facility_df.rename(columns={'Id': 'FacilityId', 'Name': 'FacilityName'}, inplace=True)
        return facility_df

    def get_opportunity_data(self, region):
        fields = ['Id', 'IsClosed', 'Name', 'CloseDate', 'IsWon', 'Opportunity_Stage__c', 'Pitching_Date__c',
                  'Negotiation_Date__c', 'AccountId', 'OwnerId', 'CreatedDate', 'Lead_Score__c', 'LeadSource',
                  'Qualified_Date_Manual__c', 'Commitment_Date__c', 'Closed_Won_Date__c', 'Nurturing_Date__c',
                  'Closed_Lost_Date__c', 'Waitlist_Date__c', 'Churned_check_for_yes__c', 'Churn_Date__c',
                  'Last_Activity__c', 'LastActivityDate', 'Kitchen_Type__c', 'Probability', 'Kitchen_City__c',
                  'Kitchen_Country__c', 'StageName', 'Facility__c', 'General_Lost_Reason__c', 'Specific_Lost_Reason__c',
                  'TourCompleted__c', 'Final_Tour_Date__c', 'Facility_Tour_Date__c',
                  'ConvertCurrency(Monthly_Shelf_Fee_Dry_Storage__c)', 'ConvertCurrency(Monthly_Shelf_Fee_Cold_Storage__c)',
                  'ConvertCurrency(Monthly_Shelf_Fee_Frozen_Storage__c)', 'SQL_Date__c', 'SDR_Attribution__c','Cuisine_Type__c']
        fields_txt = ', '.join(fields)
        query_text = f"SELECT {fields_txt} FROM Opportunity WHERE OwnerId in (SELECT Id FROM User WHERE Region__c in ('LATAM','US/CAN'))" \
        f" and (recordtypeid = '012f4000000Su90AAC' OR kitchen_type__c in ('Virtual', 'CloudRetail','virtual;cloudretail'))"

        #opportunity_data_iterator = self.sf.query_all_iter(query_text)
        opportunity_df=pd.DataFrame(self.sf.query_all(query_text)['records'])
        opportunity_df.drop(columns=['attributes'],inplace=True)
        #opportunity_df = self.iter_to_df(opportunity_data_iterator, fields)
        opportunity_df.rename(columns={'Id': 'OpportunityId',
                                       'OwnerId': 'OppOwnerId',
                                       'CreatedDate': 'QualifiedDate',
                                       'LastActivityDate': 'LastOppActivityDate',
                                       'Lead_Score__c': 'Score',
                                       'LeadSource': 'Source',
                                       'Kitchen_City__c': 'KitchenCity',
                                       'Kitchen_Country__c': 'KitchenCountry'}, inplace=True)

        opportunity_df["mrr"] = opportunity_df[['Monthly_Shelf_Fee_Dry_Storage__c',
                                                'Monthly_Shelf_Fee_Cold_Storage__c',
                                                'Monthly_Shelf_Fee_Frozen_Storage__c']].sum(axis=1)

        # convert date strings to timestamps
        opportunity_df.loc[opportunity_df['Qualified_Date_Manual__c'].notnull(), ['QualifiedDate']] = opportunity_df[
            'Qualified_Date_Manual__c']
        opportunity_df['QualifiedDate'] = opportunity_df['QualifiedDate'].apply(
            lambda x: helpers.convert_to_timestamp(x))
        opportunity_df['QualifiedDate'] = opportunity_df['QualifiedDate'].apply(
            lambda x: x.replace(hour=0, minute=0, second=0, microsecond=0))
        opportunity_df['Pitching_Date__c'] = opportunity_df['Pitching_Date__c'].apply(
            lambda x: helpers.convert_to_timestamp(x))
        opportunity_df['Negotiation_Date__c'] = opportunity_df['Negotiation_Date__c'].apply(
            lambda x: helpers.convert_to_timestamp(x))
        opportunity_df['Commitment_Date__c'] = opportunity_df['Commitment_Date__c'].apply(
            lambda x: helpers.convert_to_timestamp(x))
        opportunity_df['Closed_Won_Date__c'] = opportunity_df['Closed_Won_Date__c'].apply(
            lambda x: helpers.convert_to_timestamp(x))
        opportunity_df['Closed_Lost_Date__c'] = opportunity_df['Closed_Lost_Date__c'].apply(
            lambda x: helpers.convert_to_timestamp(x))
        opportunity_df['Nurturing_Date__c'] = opportunity_df['Nurturing_Date__c'].apply(
            lambda x: helpers.convert_to_timestamp(x))
        opportunity_df['Waitlist_Date__c'] = opportunity_df['Waitlist_Date__c'].apply(
            lambda x: helpers.convert_to_timestamp(x))
        opportunity_df['Churn_Date__c'] = opportunity_df['Churn_Date__c'].apply(
            lambda x: helpers.convert_to_timestamp(x))
        opportunity_df['Final_Tour_Date__c'] = opportunity_df['Final_Tour_Date__c'].apply(
            lambda x: helpers.convert_to_timestamp(x))

        # if closed won, make sure commitment exists
        opportunity_df.loc[
            (opportunity_df['Closed_Won_Date__c'].notnull() & opportunity_df['Commitment_Date__c'].isnull()),
            'Commitment_Date__c'] = opportunity_df['Closed_Won_Date__c']

        # if commitment, make sure negotiation exists
        opportunity_df.loc[
            opportunity_df['Commitment_Date__c'].notnull() & opportunity_df['Negotiation_Date__c'].isnull(),
            'Negotiation_Date__c'] = opportunity_df['Commitment_Date__c']

        # if negotiation, make sure pitching exists
        opportunity_df.loc[
            opportunity_df['Negotiation_Date__c'].notnull() & opportunity_df['Pitching_Date__c'].isnull(),
            'Pitching_Date__c'] = opportunity_df['Negotiation_Date__c']

        # if pitching, make sure qualified exists
        opportunity_df.loc[
            opportunity_df['Pitching_Date__c'].notnull() & opportunity_df['QualifiedDate'].isnull(),
            'QualifiedDate'] = opportunity_df['Pitching_Date__c']

        return opportunity_df

    def get_user_data(self):
        fields = ['Id', 'Name', 'Email', 'Alias', 'Title', 'City', 'Country', 'Region__c', 'IsActive',
                  'UserRoleId', 'Managers_Full_Name__c', 'CreatedDate',
                   #'ProfileId',
                  'CreatedById', 'AE_Start_Date__c', 'AE_End_Date__c', 'SDR_Start_Date__c', 'SDR_End_Date__c']

        fields_txt = ', '.join(fields)
        query_text = f"SELECT {fields_txt} FROM User"
        user_data_iterator = self.sf.query_all_iter(query_text)
        user_df = self.iter_to_df(user_data_iterator, fields)
        user_df.rename(columns={'Id': 'UserId', 'Name': 'UserName', 'CreatedDate': 'UserStartDate'}, inplace=True)
        user_df['UserStartDate'] = user_df['AE_Start_Date__c']

        # Get user region
        user_df['UserSubRegion'] = user_df[['Title', 'City', 'Country']].apply(
            lambda x: helpers.clean_region(x[0], x[1], x[2]), axis=1)
        # Update user city if San Jose
        user_df.loc[(user_df['Country'] == 'Costa Rica') & (user_df['City'] == 'San Jose'), ['City']] = 'San Jose - CR'
        # Check is ramped
        user_df['UserStartDate'] = user_df['UserStartDate'].apply(lambda x: helpers.convert_to_timestamp(x))
        user_df['IsRamped'] = user_df['UserStartDate'].apply(
            lambda x: helpers.check_is_ramped(x, datetime.datetime.now()))
        user_df['IsRamping'] = user_df['UserStartDate'].apply(
            lambda x: helpers.check_is_ramping(x, datetime.datetime.now()))
        user_df['IsOnboarding'] = False
        user_df['OnboardingStatus'] = ''
        user_df.loc[~user_df['IsRamped'] & ~user_df['IsRamping'], ['IsOnboarding']] = True
        user_df.loc[user_df['IsRamping'], ['OnboardingStatus']] = 'Ramping'
        user_df.loc[user_df['IsRamped'], ['OnboardingStatus']] = 'Ramped'
        user_df.loc[user_df['IsOnboarding'], ['OnboardingStatus']] = 'Onboarding'
        user_df.loc[~user_df['IsActive'], ['OnboardingStatus']] = 'Terminated'

        # Hack - update city for MTY -> CDMX AEs
        user_df.loc[user_df['UserName'] == 'Nancy Lozano', ['City']] = 'Mexico City'
        user_df.loc[user_df['UserName'] == 'Karin Luna', ['City']] = 'Mexico City'
        user_df.loc[user_df['UserName'] == 'Ariana Winer', ['City']] = 'Mexico City'
        user_df.loc[user_df['UserName'] == 'Victor Barco', ['City']] = 'Mexico City'
        user_df.loc[user_df['UserName'] == 'Monica Castillo', ['City']] = 'Mexico City'
        user_df.loc[user_df['UserName'] == 'Diana Ballesteros', ['City']] = 'Mexico City'
        user_df.loc[user_df['UserName'] == 'Enrique Pedroza', ['City']] = 'Mexico City'
        user_df.loc[user_df['UserName'] == 'Jesus Jasso', ['City']] = 'Monterrey'
        user_df.loc[user_df['UserName'] == 'Mauricio Rosas', ['City']] = 'Bogota'
        user_df.loc[user_df['UserName'] == 'Guilherme Bertie', ['AE_End_Date__c']] = '2020-09-21'
        user_df.loc[user_df['UserName'] == 'Guilherme Bertie', ['OnboardingStatus']] = 'Terminated'

        # Fix date field types
        user_df['AE_Start_Date__c'] = user_df['AE_Start_Date__c'].apply(lambda x: helpers.convert_to_timestamp(x))
        user_df['AE_End_Date__c'] = user_df['AE_End_Date__c'].apply(lambda x: helpers.convert_to_timestamp(x))
        user_df['SDR_Start_Date__c'] = user_df['SDR_Start_Date__c'].apply(lambda x: helpers.convert_to_timestamp(x))
        user_df['SDR_End_Date__c'] = user_df['SDR_End_Date__c'].apply(lambda x: helpers.convert_to_timestamp(x))

        # Add profile name
        #profile_df = self.get_lookup_value('Profile')
        #user_df = user_df.merge(profile_df, left_on="ProfileId", right_on="Id", how="left")
        #user_df.rename(columns={'Name': 'ProfileName'}, inplace=True)
        #user_df.drop(columns=['Id', 'ProfileId'], inplace=True)

        # Add role name
        role_df = self.get_lookup_value('UserRole')
        user_df = user_df.merge(role_df, left_on="UserRoleId", right_on="Id", how="left")
        user_df.rename(columns={'Name': 'RoleName'}, inplace=True)
        user_df.drop(columns=['Id', 'UserRoleId'], inplace=True)

        # Add created by
        created_by_df = user_df[['UserId', 'UserName']].reset_index(drop=True)
        created_by_df.rename(columns={'UserName': 'CreatedByName', 'UserId': 'NewCreatedById'}, inplace=True)
        user_df = user_df.merge(created_by_df, left_on="CreatedById", right_on="NewCreatedById", how="left")
        user_df.drop(columns=['NewCreatedById', 'CreatedById'], inplace=True)

        return user_df

    def delete_leads(self, df, id_column):
        for index, row in df.iterrows():
            self.sf.Lead.delete(row[id_column])
            print(row[id_column], 'Deleted')

    def get_region_data(self, region):
        print("Getting User Data ...")
        # Get user data
        global_user_data = self.get_user_data()
        print("Getting Facility Data ...")
        # Get user data and send to csv
        global_facility_data = self.get_facility_data()
        print("Getting Leads Data ...")
        region_lead_data = self.get_lead_data(region)

        # Get opportunity data
        print("Getting Opportunity Data ...")
        region_opportunity_data = self.get_opportunity_data(region)

        # Get data for Latam
        print("Merging Global Data ...")
        region_data = helpers.merge_sf_data(global_user_data, region_lead_data, region_opportunity_data,
                                            global_facility_data)
        print("Global Data Pulled")
        return region_data

    def get_weekly_activity_data(self, user_ids, week):
        # Get activity data
        print("Getting Weekly Activity Data ...")
        activity_data = self.get_activity_by_owner_ids(user_ids)
        week_activity_data = activity_data[activity_data['Week'] == week].reset_index(drop=True)
        return week_activity_data

    def get_monthly_activity_data(self, user_ids, month):
        # Get activity data
        print("Getting Monthly Activity Data ...")
        activity_data = self.get_activity_by_owner_ids(user_ids)
        month_activity_data = activity_data[activity_data['Month'] == month].reset_index(drop=True)
        return month_activity_data

    def __init__(self, path):
        self.loginInfo = json.load(open(path))
        self.session_id, self.instance = SalesforceLogin(username=self.loginInfo['username'],
                                                         password=self.loginInfo['password'],
                                                         security_token=self.loginInfo['security_token'],
                                                         domain='login')
        self.sf = Salesforce(instance=self.instance, session_id=self.session_id)
