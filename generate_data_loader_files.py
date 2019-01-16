import os.path
import pandas as pd
import sys


def generate_bounced_output(df, outdir):
    bounced_df = df[df['ActivityType__c'].str.startswith('Bounced')][['ca id']].copy()
    bounced_df = bounced_df.rename(columns={'ca id': 'Id'})
    bounced_df['Bounced__c'] = 'TRUE'
    bounced_df.to_csv(os.path.join(outdir, 'bounced.csv'), index=False)


def generate_suppressed_output(df, outdir):
    suppressed_df = df[df['MessageName__c'] == 'Suppressed'][['Contact__c']].copy()
    suppressed_df = suppressed_df.rename(columns={'Contact__c': 'Id'})
    suppressed_df['SuppressedInRM__c'] = 'TRUE'
    suppressed_df[['Id', 'SuppressedInRM__c']].to_csv(os.path.join(outdir, 'suppressed.csv'), index=False)


def generate_nca_output(df, outdir):
    nca_df = df[df['ActivityType__c'] == 'Usnsubscribe From All'][['Contact__c']].copy()
    nca_df = nca_df.rename(columns={'Contact__c': 'Id'})
    nca_df['NeverContactAgainReason__c'] = 'Global Opt-Out(RM)'
    nca_df.to_csv(os.path.join(outdir, 'nca.csv'), index=False)


def generate_unsubscribed_output(df, outdir):
    unsubscribed_df = df[df['ActivityType__c'] == 'Unsubscribed'][['ca id']].copy()
    unsubscribed_df.rename(columns={'ca id': 'Id'})
    unsubscribed_df['Unsubscribed__c'] = 'TRUE'
    unsubscribed_df['Status__c'] = 'Not Interested'
    unsubscribed_df.to_csv(os.path.join(outdir, 'unsubscribed.csv'), index=False)


def generate_rma_output(df, outdir):
    columns = [
        'MessageName__c',
        'EmailAddress__c',
        'Contact__c',
        'CompositeKey__c',
        'Timestamp__c',
        'LinkURL__c',
        'ActivityType__c',
        'CandidateAssignmentAuxiliary__c']
    rma_df = df[columns].copy()
    rma_df = rma_df.drop_duplicates(subset='CompositeKey__c')
    rma_df.to_csv(os.path.join(outdir, 'rma.csv'), index=False)


def generate_converted_rmah_output(df, outdir):
    df['RMAConversionStatus__c'] = 'Complete'
    df[['Id', 'RMAConversionStatus__c']].to_csv(os.path.join(outdir, 'rmah.csv'), index=False)


def get_input_dfs(rmah_in, caa_in):
    rmah_df = pd.read_csv(rmah_in)
    caa_df = pd.read_csv(caa_in)
    rmah_df = rmah_df.rename(columns={
        'Real_Magnet__Activity_Subcode__r.Name': 'ActivityType__c',
        'Name': 'MessageName__c',
        'Real_Magnet__Email_Address__c': 'EmailAddress__c',
        'Real_Magnet__Contact__c': 'Contact__c',
        'Real_Magnet__Activity_Date__c': 'Timestamp__c',
        'Real_Magnet__Link_URL__c': 'LinkURL__c',
    })
    caa_df = caa_df.rename(columns={
        'Id': 'CandidateAssignmentAuxiliary__c',
        'CandidateAssignment__c': 'ca id',
    })
    return rmah_df, caa_df


def generate_contact_update_files(df, outdir):
    generate_nca_output(df, outdir)
    generate_suppressed_output(df, outdir)


def generate_ca_output_files(df, outdir):
    generate_bounced_output(df, outdir)
    generate_unsubscribed_output(df, outdir)


def generate_real_magnet_upsert_and_update_files(df, outdir):
    generate_rma_output(df, outdir)
    generate_converted_rmah_output(df, outdir)


def generate_all_files(rmah_in, caa_in, outdir):
    rmah_df, caa_df = get_input_dfs(rmah_in, caa_in)
    generate_contact_update_files(rmah_df, outdir)
    df = rmah_df.merge(caa_df, on='MasterCompositeKey__c')
    generate_ca_output_files(df, outdir)
    generate_real_magnet_upsert_and_update_files(df, outdir)


if __name__ == '__main__':
    generate_all_files(sys.argv[1], sys.argv[2], sys.argv[3])
