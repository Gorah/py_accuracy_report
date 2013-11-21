#Script written for Python 2.7
#Dependencies to download: pyodbc (SQL Server Drivers)

import pyodbc
import datetime
import sys
from contextlib import contextmanager

LATE_CASES = {}

@contextmanager
def get_connection():    
    """
    Connect to DB
    """
    cnxn = pyodbc.connect('DRIVER={SQL SERVER};SERVER=BPOPLMCBC16;DATABASE=AdminTracker_SQL;UID=AppLogon;PWD=ZdrojHPL1950')
    yield cnxn.cursor()
    cnxn.commit()



def get_DBdata(sql, sD, eD, cursor):
    """
    This function takes SQL string and connection object and returns
    rowset with data
    """

    cursor.execute(sql, sD, eD)
    try:
        rows = cursor.fetchall()
    except Error as err:
        rows = None

    return rows


def count_days(row):
    """
    This function calculates number of days between Cut Off Date and
    date of receiving complete documents.
    """
    cutD = datetime.datetime.strptime(row.CutOffDate,
                                      '%Y-%m-%d %H:%M:%S')

    #if CompleteDocsDate is missing, use current date instead
    if row.CompleteDocsDate:
        recD = datetime.datetime.strptime(row.CompleteDocsDate,
                                      '%Y-%m-%d %H:%M:%S')
    else:
        recD = datetime.datetime.now()

    days = (recD - cutD).days
    #return number of days or 0 if number of days is negative
    if days > 0:
        return  days
    else:
        return 0


def write_to_dict(row, ttype, notes_name, docs_rec, notes_override):
    """
    This function fills dictionary with a new entry (which is another
    dictionary containing all the necessary data)
    """

    #new empty dictionary is created to store ticket data in it
    case_descr = {}
    case_descr['type'] = ttype
    #This allows overriding default notes script overriding
    if notes_override:
        case_descr['notes'] = notes_override
    else:    
        case_descr['notes'] = notes_name + row.EffectiveDate
        + docs_rec + '. Days late for payroll cut off: ' + count_days(row)
        + '. ' + row.EEImpact + '.'
        
    case_descr['eename'] = row.Surname + ' ' + row.Forename  
    case_descr['eeid'] = row.EEID
    case_descr['hrbp'] = row.HRBP

    #new dictionary is appended to general dict under ticket ID as key
    LATE_CASES[row.ID] = case_descr
    

def contract_exp_by_dates(sD, eD, cursor):
    """
    This function takes date boundaries and connection object and
    fetches data from DB. Then it sends recordsets for further
    processing.
    This function covers Contract Expiration - Late Submission category.
    """
    sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate,
             T.CutOffDate, T.EEImpact, T.CompleteDocsDate, 
             T.NumberOfReminders, T.HRBP, E.EEID, E.Forname, E.Surname 
             FROM tTracker as T INNER JOIN
             tMCBCEmployee as E ON T.EeID = E.ID
             WHERE (T.ProcessID IN (262, 330)) AND
             (T.DateReceived BETWEEN ? AND ?) AND 
             (T.CurrentStatus IN (1, 9)) AND
             ((T.EffectiveDate < T.DateReceived) OR
             (T.CutOffDate < T.DateReceived))"""
    ttype = 'Contract Expiration - Late Renewal Submission'
    notes_name = 'Contract End date '
    notes_override = None

    #getting recordset from DB    
    result = get_DBdata(sql, sD, eD, cursor)

    """if there are any rows in response we're checking each row to
    determine which piece of string to use in description of case.
    After string is determined row and meta data are sent to be added
    to dictionary.
    """   
    if result:
        for row in result:
            if row.CompleteDocsDate:
                docs_rec = '. Complete documents received on ' + row.CompleteDocsDate
            else:
                docs_rec = '. Complete documenst still pending'
                
            write_to_dict(row, ttype, notes_name, docs_rec, notes_override)


def contract_exp_by_letters(sD, eD, cursor):
    """
    This function fetches data for Contract Expiration category,scoping
    for the letter tickets. Late tickets are fetched and are split into
    2 different types: 'no response' and 'late submission'.
    Data is later sent to be written to dictionary
    """
    sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate, 
             T.CutOffDate, T.EEImpact, T.CompleteDocsDate, 
             T.NumberOfReminders, T.HRBP, E.EEID, E.Forname, 
             E.Surname, T.LetterReceived FROM tTracker as T INNER JOIN 
             tMCBCEmployee as E ON T.EeID = E.ID 
             WHERE (T.ProcessID IN (349, 351, 352, 350, 383, 399)) AND 
             (T.DateReceived BETWEEN ? AND ?) AND
             (T.CurrentStatus IN (1, 9)) AND
             ((T.EffectiveDate < T.CompleteDocsDate) OR
             (T.CutOffDate < T.CompleteDocsDate) 
              OR (T.EffectiveDate < GETDATE()
              AND T.LetterReceived = 0 ) OR (T.CutOffDate < GETDATE()
              AND T.LetterReceived = 0 ))"""
    notes_name = 'Contract End date '
    docs_rec = ''
    notes_override = None

    #getting recordset from DB
    result = get_DBdata(sql, sD, eD, cursor)
    
    """if there are any rows in response we're checking each row to
    determine which piece of string to use in description of case.
    After string is determined row and meta data are sent to be added
    to dictionary.
    """
    if result:
        for row in result:
            if row.LetterReceived = 0:
                ttype = 'Contract Expiration - No Response'
            else:
                ttype = 'Contract Expiration - Late Renewal Submission'

            write_to_dict(row, ttype, notes_name, docs_rec, notes_override)


def loa_by_dates(sD, eD, cursor):
    """
    This function collects data about LOA category and sends records
    with late tickets to be added to dictionary.
    """
    sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate, 
             T.CutOffDate, T.EEImpact, T.CompleteDocsDate,
             T.NumberOfReminders, T.HRBP, E.EEID, E.Forname, E.Surname 
             FROM tTracker as T INNER JOIN
             tMCBCEmployee as E ON T.EeID = E.ID
             WHERE (T.ProcessID IN (246, 261, 264, 282, 284, 289, 305,
             306, 326, 341)) AND 
             (T.DateReceived BETWEEN ? AND ?) AND (T.CurrentStatus IN
             (1, 9)) AND ((T.EffectiveDate < T.DateReceived) OR
             (T.CutOffDate < T.DateReceived))"""
    notes_name = 'LOA effective '
    ttype = 'Leave of Absence - Late Submission'
    docs_rec = '. PCR Received on '
    notes_override = None

    #getting recordset from DB
    result = get_DBdata(sql, sD, eD, cursor)

    #if there are any records in the recordset each row is sent to be
    #added to dictionary
    if result:
        for row in result:
            write_to_dict(row, ttype, notes_name, docs_rec, notes_override)


def ret_from_loa_by_dates(sD, eD, cursor):            
    """
    This function collects data about Return From LOA category and
    sends records with late tickets to be added to dictionary.
    """
    sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate, 
             T.CutOffDate, T.EEImpact, T.CompleteDocsDate,
             T.NumberOfReminders, T.HRBP, E.EEID, E.Forname, E.Surname 
             FROM tTracker as T INNER JOIN
             tMCBCEmployee as E ON T.EeID = E.ID
             WHERE (T.ProcessID = 325)) AND 
             (T.DateReceived BETWEEN ? AND ?) AND (T.CurrentStatus IN
             (1, 9)) AND ((T.EffectiveDate < T.DateReceived) OR
             (T.CutOffDate < T.DateReceived))"""
    notes_name = 'Return effective '
    ttype = 'Return from Leave - Late Submission'
    docs_rec = '. PCR Received on '
    notes_override = None

    #getting recordset from DB
    result = get_DBdata(sql, sD, eD, cursor)

    #if there are any records in the recordset each row is sent to be
    #added to dictionary
    if result:
        for row in result:
            write_to_dict(row, ttype, notes_name, docs_rec, notes_override)
    

def late_by_dates_missingdocs(sD, eD, scope, procname, cursor):
    """
    This generic function collects data about specified category and
    sends records with late tickets to be added to dictionary.

    It takes into consideration action in SAP tickets with complete
    documentation not yet received.
    """
    sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate,
             T.CutOffDate, T.EEImpact, T.CompleteDocsDate,
             T.NumberOfReminders, T.HRBP, E.EEID, E.Forname, E.Surname 
             FROM tTracker as T INNER JOIN
             tMCBCEmployee as E ON T.EeID = E.ID
             WHERE (T.ProcessID IN (""" + scope + """)) AND 
             (T.DateReceived BETWEEN ? AND ?) AND (T.CurrentStatus IN (1, 9)) 
             AND ((T.EffectiveDate < T.DateReceived) OR
             (T.CutOffDate < T.DateReceived)) AND T.CompleteDocsDate
             IS NULL"""
     notes_name = procname + ' effective '
     ttype = procname + ' - Late Submission'
     docs_rec = '. Complete info still pending'
     notes_override = None

     #getting recordset from DB
     result = get_DBdata(sql, sD, eD, cursor)

     #if there are any records in the recordset each row is sent to be
     #added to dictionary
     if result:
         for row in result:
             write_to_dict(row, ttype, notes_name, docs_rec, notes_override)
             
             
def late_by_dates_completedoc(sD, eD, scope, procname, cursor):
    """
    This generic function collects data about specified category and
    sends records with late tickets to be added to dictionary.

    It takes into consideration action in SAP tickets complete
    documentation received.
    """
    sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate,
             T.CutOffDate, T.EEImpact, T.CompleteDocsDate,
             T.NumberOfReminders, T.HRBP, E.EEID, E.Forname, E.Surname 
             FROM tTracker as T INNER JOIN
             tMCBCEmployee as E ON T.EeID = E.ID
             WHERE (T.ProcessID IN (""" + scope + """)) AND 
             (T.DateReceived BETWEEN ? AND ?) AND (T.CurrentStatus IN (1, 9)) 
             AND ((T.EffectiveDate < T.CompleteDocsDate) OR
             (T.CutOffDate < T.CompleteDocsDate)) AND T.CompleteDocsDate
             IS NOT NULL"""
    notes_name = procname + ' effective '
    ttype = procname + ' - Late Submission'
    notes_override = None

    #getting recordset from DB
    result = get_DBdata(sql, sD, eD, cursor)

    #if there are any records in the recordset each row is sent to be
    #added to dictionary
    if result:
        for row in result:
            docs_rec = '. Complete info received on ' + row.CompleteDocsDate
            write_to_dict(row, ttype, notes_name, docs_rec, notes_override)


def late_by_letters(sD, eD, scope, procname, cursor):
    """
    This function finds all the New Hire contracts with missing info
    and loads them to dict.
    """
     sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate, 
             T.CutOffDate, T.EEImpact, T.CompleteDocsDate, 
             T.NumberOfReminders, T.HRBP, E.EEID, E.Forname, 
             E.Surname, T.LetterReceived FROM tTracker as T INNER JOIN 
             tMCBCEmployee as E ON T.EeID = E.ID 
             WHERE (T.ProcessID IN (""" + scope + """)) AND
             (T.DateReceived BETWEEN ? AND ?) AND (T.CurrentStatus IN
             (1, 9)) AND ((T.EffectiveDate < T.CompleteDocsDate) OR
             (T.CutOffDate < T.CompleteDocsDate) OR (T.EffectiveDate < GETDATE()
              AND T.LetterReceived = 0 ) OR (T.CutOffDate < GETDATE()
              AND T.LetterReceived = 0 ))"""
    notes_name = procname + ' effective '
    ttype = procname + ' - Missing Documentation'
    docs_rec = '. Complete info still pending'
    notes_override = None

    #getting recordset from DB
    result = get_DBdata(sql, sD, eD, cursor)

    #if there are any records in the recordset each row is sent to be
    #added to dictionary
    if result:
        for row in result:
            write_to_dict(row, ttype, notes_name, docs_rec, notes_override)


def termination_checklist_check(cursor):
    """
    This function finds all unsubmitted termination checklists and
    feeds them into dictionary.
    """
    sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate, 
             T.CutOffDate, T.EEImpact, T.CompleteDocsDate, 
             T.NumberOfReminders, T.HRBP, E.EEID, E.Forname, 
             E.Surname, T.LetterReceived FROM tTracker as T INNER JOIN 
             tMCBCEmployee as E ON T.EeID = E.ID 
             WHERE (T.ProcessID = 417) AND (T.LetterReceived = 0)"""
    notes_name = ''
    ttype = 'Termination -  No Termination Checklist submitted'
    docs_rec = ''
    notes_override = 'Possible SOX audit compliance issue'
    
    #getting recordset from DB
    result = get_DBdata(sql, sD, eD, cursor)

    #if there are any records in the recordset each row is sent to be
    #added to dictionary
    if result:
        for row in result:
            write_to_dict(row, ttype, notes_name, docs_rec, notes_override)
            

def write_to_file():
    """
    This function saves report to csv file
    """
    
    #Open file to save report to
    report = open('report.csv', 'w')
    for key in LATE_CASES:
        #build file entry row from dict data
        fileentry = key + ',' + LATE_CASES[key]['type'] + ','
        + LATE_CASES[key]['notes'] + ',' + LATE_CASES[key]['eename']
        + ',' + LATE_CASES[key]['eeid'] + ',' + LATE_CASES[key]['hrbp']

        #write etry to file
        report.write(fileentry + '\n')

    #close the file    
    report.close()
    
            
def runReport(sD, eD):

    with get_connection() as cursor:
        #Contract Expiration section
        contract_exp_by_dates(sD, eD, cursor)
        contract_exp_by_letters(sD, eD, cursor)
        #LOA section
        loa_by_dates(sD, eD, cursor)
        #Return From LOA section
        ret_from_loa_by_dates(sD, eD, cursor)

        #Job Change section
        procname = 'Job Change'
        #Job Changes action tickets
        scope = '315, 331, 323, 335, 340, 339'
        late_by_dates_missingdocs(sD, eD, scope, procname, cursor)
        late_by_dates_completedoc(sD, eD, scope, procname, cursor)
        #Job Changes letter tickets
        scope = '363, 385, 386, 400, 399, 410, 412, 413'
        late_by_letters(sD, eD, scope, procname, cursor)

        #New Hire section
        procname = 'Hire'
        #New Hire tickets
        scope = '371, 372'
        late_by_letters(sD, eD, scope, procname, cursor)

        #Pay Changes section
        procname = 'Pay Change'
        #Pay Changes action tickets
        scope = '327, 328, 329'
        late_by_dates_missingdocs(sD, eD, scope, procname, cursor)
        late_by_dates_completedoc(sD, eD, scope, procname, cursor)
        #Pay Changes letter tickets
        scope = '395, 396, 397, 347'
        late_by_letters(sD, eD, scope, procname, cursor)


        #Termination section
        procname = 'Termination'
        #Termination actions
        scope = '336, 337, 338'
        late_by_dates_completedoc(sD, eD, scope, procname, cursor)
        late_by_dates_missingdocs(sD, eD, scope, procname, cursor)
        #Termination checklist
        termination_checklist_check(cursor)

        #Save the report to file
        write_to_file()

        
        

        
