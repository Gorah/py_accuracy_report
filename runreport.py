#Script written for Python 2.7
#Dependencies to download: pyodbc (SQL Server Drivers)

import pyodbc
import datetime
import sys
import re
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
    if(sD):
        cursor.execute(sql, sD, eD)
    else:
        cursor.execute(sql)
        
    try:
        rows = cursor.fetchall()
    except Error as err:
        rows = None
        print err.strerror
        sys.exit(0)

    return rows


def count_days(row, userecdate=False):
    """
    This function calculates number of days between Cut Off Date and
    date of receiving complete documents.
    """
    cutD = row.CutOffDate

    if userecdate:
        recD = row.DateReceived
    else:    
        #if CompleteDocsDate is missing, use current date instead
        try:
            recD = row.CompleteDocsDate
        except AttributeError:
            recD = datetime.datetime.now()
            
        if not recD:
            recD = datetime.datetime.now()
        
    return day_diff(recD, cutD)
            

def day_diff(date1, date2):
    """
    This function returns difference in days between 2 dates.
    """

    days = (date1 - date2).days +1
    if days > 0:
        return days
    else:
        return 0

    

def write_to_dict(row, ttype, notes_name, docs_rec, notes_override, userecdate, skip_date_rec = False):
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
    elif skip_date_rec:
        if not row.CutOffDate:
            cutoff = datetime.datetime.now().strftime('%d/%m/%Y')
        else:
            cutoff = row.CutOffDate.strftime('%d/%m/%Y')
            
        case_descr['notes'] = ('"%s%s%s.\n%s%s.\nDays late for payroll cut off: %d.\n%s."' %
                               (notes_name, row.EffectiveDate.strftime('%d/%m/%Y'),
                                docs_rec, 'Request should by submitted by ',
                                cutoff, count_days(row, userecdate), row.EEImpact))
    else:
        if not row.CutOffDate:
            cutoff = datetime.datetime.now().strftime('%d/%m/%Y')
        else:
            cutoff = row.CutOffDate.strftime('%d/%m/%Y')
            
        case_descr['notes'] = ('"%s%s%s%s.\n%s%s.\nDays late for payroll cut off: %d.\n%s."' %
                               (notes_name,
                                row.EffectiveDate.strftime('%d/%m/%Y'),
                                docs_rec,
                                row.DateReceived.strftime('%d/%m/%Y'),
                                'Request should by submitted by ',
                                cutoff,
                                count_days(row, userecdate),
                                row.EEImpact)
        )
        
    if not row.Forname:
        forename = ' '
    else:
        forename = row.Forname
    case_descr['eename'] = row.Surname + ' ' + forename  
    case_descr['eeid'] = row.EEID

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
             T.NumberOfReminders, E.EEID, E.Forname, E.Surname 
             FROM tTracker as T INNER JOIN
             tMCBCEmployee as E ON T.EeID = E.ID
             WHERE (T.ProcessID IN (262, 330)) AND
             (T.DateReceived BETWEEN ? AND ?) AND 
             (T.EffectiveDate < T.DateReceived OR T.CutOffDate < T.DateReceived)"""
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
                docs_rec = '.\nComplete documents received on '
            else:
                docs_rec = '.\nComplete documenst still pending'

            write_to_dict(row, ttype, notes_name, docs_rec, notes_override, False, True)                
            


def contract_exp_by_letters(sD, eD, cursor):
    """
    This function fetches data for Contract Expiration category,scoping
    for the letter tickets. Late tickets are fetched and are split into
    2 different types: 'no response' and 'late submission'.
    Data is later sent to be written to dictionary
    """
    sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate, 
             T.CutOffDate, T.EEImpact, T.CompleteDocsDate, 
             T.NumberOfReminders, E.EEID, E.Forname, 
             E.Surname, T.LetterReceived FROM tTracker as T INNER JOIN 
             tMCBCEmployee as E ON T.EeID = E.ID 
             WHERE T.ProcessID IN (349, 351, 352, 350, 383, 399) AND 
             (T.DateReceived BETWEEN ? AND ?) AND
             (T.EffectiveDate < T.CompleteDocsDate OR
             T.CutOffDate < T.CompleteDocsDate OR (T.EffectiveDate < GETDATE()
              AND T.LetterReceived = 0 ) OR (T.CutOffDate < GETDATE()
              AND T.LetterReceived = 0 )) """
    notes_name = 'Contract End date '
    docs_rec = '\nPCR received on '
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
            if row.LetterReceived == 0:
                ttype = 'Contract Expiration - No Response'
                today = datetime.datetime.now()
                notes_override = ('"%s%s.\n%s%s.\n%s%d.\n%s"' % ('Contract End date ',
                                                               row.EffectiveDate.strftime('%d/%m/%Y'),
                                                               'Request should be submitted by ',
                                                               row.CutOffDate.strftime('%d/%m/%Y'),
                                                               'Days late for payroll cut off: ',
                                                               day_diff(today, row.CutOffDate),
                                                               row.EEImpact))
            else:
                ttype = 'Contract Expiration - Late Renewal Submission'
                if row.CompleteDocsDate:
                    docs_rec = ('%s%s' % ('.\nComplete documents received on ',
                                          row.CompleteDocsDate.strftime('%d/%m/%Y')))
                else:
                    docs_rec = '.\nComplete documenst still pending'

            write_to_dict(row, ttype, notes_name, docs_rec, notes_override, False)


def late_loa(sD, eD, cursor):
    """
    This function finds late loa cases
    """
    sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate, 
             T.EEImpact, E.EEID, E.Forname, E.Surname, P.ProcessName 
             FROM tTracker as T INNER JOIN
             tMCBCEmployee as E ON T.EeID = E.ID INNER JOIN
             tProcess as P ON T.ProcessID = P.ID
             WHERE (T.ProcessID IN (246, 261, 264, 282, 284, 289, 305,
             306, 326, 341)) AND 
             (T.DateReceived BETWEEN ? AND ?)"""
    ttype = 'Leave of Absence - Late Submission'
    notes_name = None
    docs_rec = None

    #getting recordset from DB
    result = get_DBdata(sql, sD, eD, cursor)

    #if there are any records in the recordset they need to be analized if
    #they are late.
    if result:
        for row in result:
            #checks if row is late. if yes adds an entry
            if check_if_late_loa(row):
                friday = row.EffectiveDate + datetime.timedelta(days=(4 - row.EffectiveDate.weekday()))
                notes_override = ('"%s effective %s.\n%s%s.\n%s%s.\n%s%d.\n%s"' % (row.ProcessName,
                                                                            row.EffectiveDate.strftime('%d/%m/%Y'),
                                                                            'Request should be submitted by ',
                                                                            friday.strftime('%d/%m/%Y'),
                                                                            'Request received on ',
                                                                            row.DateReceived.strftime('%d/%m/%Y'),
                                                                            'Days late: ',
                                                                            day_diff(row.DateReceived, friday),
                                                                            row.EEImpact))
                write_to_dict(row, ttype, notes_name, docs_rec, notes_override, True)


def check_if_late_loa(row):
    """
    This function checks if loa entry is late or not based on business req.
    """

    #find how many days friday is away from
    diff = 4 - row.EffectiveDate.weekday()
    fridayDate = row.EffectiveDate + datetime.timedelta(days=diff)

    #checks if date received is greater than date of Friday in the week when
    #effective date took place
    if (row.DateReceived - fridayDate).days > 0:
        return True
    else:
        return False
    

def ret_from_loa_by_dates(sD, eD, cursor):            
    """
    This function collects data about Return From LOA category and
    sends records with late tickets to be added to dictionary.
    """
    sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate, 
             T.CutOffDate, T.EEImpact, T.CompleteDocsDate,
             T.NumberOfReminders, E.EEID, E.Forname, E.Surname 
             FROM tTracker as T INNER JOIN
             tMCBCEmployee as E ON T.EeID = E.ID
             WHERE (T.ProcessID = 325) AND 
             (T.DateReceived BETWEEN ? AND ?) AND (T.EffectiveDate < T.DateReceived)"""
    notes_name = 'Return effective '
    ttype = 'Return from Leave - Late Submission'
    docs_rec = '.\nPCR Received on '
    notes_override = None

    #getting recordset from DB
    result = get_DBdata(sql, sD, eD, cursor)

    #if there are any records in the recordset each row is sent to be
    #added to dictionary
    if result:
        for row in result:
            if (row.DateReceived - row.EffectiveDate).days > 0:
                notes_override = ('"%s%s.\n%s%s.\n%s%s.\n%s%d.\n%s"' %('Return effective on ',
                                                                       row.EffectiveDate.strftime('%d/%m/%Y'),
                                                                       'PCR Received on ',
                                                                       row.DateReceived.strftime('%d/%m/%Y'),
                                                                       'Request should be submitted by ',
                                                                       row.EffectiveDate.strftime('%d/%m/%Y'),
                                                                       'Days late for payroll cut off: ',
                                                                       day_diff(row.DateReceived, row.EffectiveDate),
                                                                       row.EEImpact
                                                                   ))
                write_to_dict(row, ttype, notes_name, docs_rec, notes_override, True)

    

def late_by_dates_missingdocs(sD, eD, scope, procname, cursor):
    """
    This generic function collects data about specified category and
    sends records with late tickets to be added to dictionary.

    It takes into consideration action in SAP tickets with complete
    documentation not yet received.
    """
    sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate,
             T.CutOffDate, T.EEImpact, T.CompleteDocsDate,
             T.NumberOfReminders, E.EEID, E.Forname, E.Surname 
             FROM tTracker as T INNER JOIN
             tMCBCEmployee as E ON T.EeID = E.ID
             WHERE (T.ProcessID IN (""" + scope + """)) AND 
             (T.DateReceived BETWEEN ? AND ?) AND (T.CurrentStatus IN (1, 9)) 
             AND ((T.EffectiveDate < T.DateReceived) OR
             (T.CutOffDate < T.DateReceived)) AND T.CompleteDocsDate
             IS NULL"""
    notes_name = procname + ' effective '
    ttype = procname + ' - Late Submission'
    docs_rec = '.\nComplete info still pending'
    notes_override = None

    #getting recordset from DB
    result = get_DBdata(sql, sD, eD, cursor)

    #if there are any records in the recordset each row is sent to be
    #added to dictionary
    if result:
        for row in result:
            write_to_dict(row, ttype, notes_name, docs_rec, notes_override, False, True)
             
             
def late_by_dates_completedoc(sD, eD, scope, procname, cursor):
    """
    This generic function collects data about specified category and
    sends records with late tickets to be added to dictionary.

    It takes into consideration action in SAP tickets complete
    documentation received.
    """
    sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate,
             T.CutOffDate, T.EEImpact, T.DocsReceivedDate,
             T.NumberOfReminders, E.EEID, E.Forname, E.Surname 
             FROM tTracker as T INNER JOIN
             tMCBCEmployee as E ON T.EeID = E.ID
             WHERE (T.ProcessID IN (""" + scope + """)) AND 
             (T.DateReceived BETWEEN ? AND ?) AND (T.CurrentStatus IN (1, 9)) 
             AND ((T.EffectiveDate < T.DocsReceivedDate) OR
             (T.CutOffDate < T.DocsReceivedDate)) AND T.DocsReceivedDate
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
            docs_rec = '.\nComplete info received on ' + row.DocsReceivedDate.strftime('%d/%m/%Y')
            write_to_dict(row, ttype, notes_name, docs_rec, notes_override, False)


def late_by_letters(sD, eD, scope, procname, cursor):
    """
    This function finds all the New Hire contracts with missing info
    and loads them to dict.
    """
    sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate, 
             T.CutOffDate, T.EEImpact, T.DocsReceivedDate, 
             T.NumberOfReminders, E.EEID, E.Forname, 
             E.Surname, T.LetterReceived FROM tTracker as T INNER JOIN 
             tMCBCEmployee as E ON T.EeID = E.ID 
             WHERE (T.ProcessID IN (""" + scope + """)) AND
             (T.DateReceived BETWEEN ? AND ?) AND (T.CurrentStatus IN
             (1, 9)) AND ((T.EffectiveDate < T.DocsReceivedDate) OR
             (T.CutOffDate < T.DocsReceivedDate) OR (T.EffectiveDate < GETDATE()
              AND T.LetterReceived = 0 ) OR (T.CutOffDate < GETDATE()
              AND T.LetterReceived = 0 ))"""
    notes_name = procname + ' effective '
    ttype = procname
    docs_rec = '.\nComplete info still pending'
    notes_override = None

    #getting recordset from DB
    result = get_DBdata(sql, sD, eD, cursor)

    #if there are any records in the recordset each row is sent to be
    #added to dictionary
    if result:
        for row in result:
            write_to_dict(row, ttype, notes_name, docs_rec, notes_override, False, True)

def termination_complete_docs(sD, eD, cursor):
    """
    This generic function collects data about specified category and
    sends records with late tickets to be added to dictionary.

    It takes into consideration action in SAP tickets complete
    documentation received.
    """
    sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate,
             T.CutOffDate, T.EEImpact, T.CompleteDocsDate,
             T.NumberOfReminders, E.EEID, E.Forname, E.Surname 
             FROM tTracker as T LEFT JOIN
             tMCBCEmployee as E ON T.EeID = E.ID
             WHERE T.ProcessID IN (336, 337, 338) AND 
             (T.DateReceived BETWEEN ? AND ?) 
             AND (T.EffectiveDate <= T.DateReceived OR
             (T.CutOffDate < T.DateReceived)) AND T.CompleteDocsDate
             IS NOT NULL """
    notes_name = 'Termination effective '
    ttype = 'Termination - Late Submission'
    notes_override = None

    #getting recordset from DB
    result = get_DBdata(sql, sD, eD, cursor)

    #if there are any records in the recordset each row is sent to be
    #added to dictionary
    if result:
        for row in result:
            docs_rec = '.\nComplete info received on '
            write_to_dict(row, ttype, notes_name, docs_rec, notes_override, False)

def termination_missing_docs(sD, eD, cursor):
    """
    This generic function collects data about specified category and
    sends records with late tickets to be added to dictionary.

    It takes into consideration action in SAP tickets with complete
    documentation not yet received.
    """
    sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate,
             T.CutOffDate, T.EEImpact, T.CompleteDocsDate,
             T.NumberOfReminders, E.EEID, E.Forname, E.Surname 
             FROM tTracker as T LEFT JOIN
             tMCBCEmployee as E ON T.EeID = E.ID
             WHERE T.ProcessID IN (336, 337, 338) AND 
             (T.DateReceived BETWEEN ? AND ?) 
             AND (T.EffectiveDate <= T.DateReceived OR
             (T.CutOffDate < T.DateReceived)) AND T.CompleteDocsDate
             IS NULL"""
    notes_name = 'Termination effective '
    ttype = 'Termination - Late Submission'
    docs_rec = '.\nComplete info still pending'
    notes_override = None

    #getting recordset from DB
    result = get_DBdata(sql, sD, eD, cursor)

    #if there are any records in the recordset each row is sent to be
    #added to dictionary
    if result:
        for row in result:
            write_to_dict(row, ttype, notes_name, docs_rec, notes_override, False, True)
    

def termination_checklist_check(cursor):
    """
    This function finds all unsubmitted termination checklists and
    feeds them into dictionary.
    """
    sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate, 
             T.CutOffDate, T.EEImpact, T.CompleteDocsDate, 
             T.NumberOfReminders, E.EEID, E.Forname, 
             E.Surname, T.LetterReceived FROM tTracker as T INNER JOIN 
             tMCBCEmployee as E ON T.EeID = E.ID 
             WHERE (T.ProcessID = 417) AND (T.LetterReceived = 0)"""
    notes_name = ''
    ttype = 'Termination -  No Termination Checklist submitted'
    docs_rec = ''
    notes_override = 'Possible SOX audit compliance issue'
    
    #getting recordset from DB
    sD = None
    eD = None
    result = get_DBdata(sql, sD, eD, cursor)

    #if there are any records in the recordset each row is sent to be
    #added to dictionary
    if result:
        for row in result:
            write_to_dict(row, ttype, notes_name, docs_rec, notes_override, False)
            

def write_to_file():
    """
    This function saves report to csv file
    """
    
    #Open file to save report to
    report = open('report.csv', 'w')
    for key in LATE_CASES:
        #build file entry row from dict data
        
        fileentry = '%d,%s,%s,%s,%d' % (key, LATE_CASES[key]['type'],
                                            LATE_CASES[key]['notes'],
                                            LATE_CASES[key]['eename'],
                                            LATE_CASES[key]['eeid'])
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
        late_loa(sD, eD, cursor)
        #Return From LOA section
        ret_from_loa_by_dates(sD, eD, cursor)

        #Job Change section
        procname = 'Job Change'
        #Job Changes action tickets
        scope = '315, 331, 323, 335, 340, 339'
        late_by_dates_missingdocs(sD, eD, scope, procname, cursor)
        late_by_dates_completedoc(sD, eD, scope, procname, cursor)
        #Job Changes letter tickets
        scope = '363, 385, 386, 400, 410, 412, 413'
        procname = 'Job Change - Late Submission'
        late_by_letters(sD, eD, scope, procname, cursor)

        #New Hire section
        procname = 'Hires - Missing Documentation'
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
        procname = 'Pay Change - Late Submission'
        scope = '395, 396, 397, 347'
        late_by_letters(sD, eD, scope, procname, cursor)


        #Termination section
        procname = 'Termination'
        #Termination actions
        termination_missing_docs(sD, eD, cursor)
        termination_complete_docs(sD, eD, cursor)
        #Termination checklist
        termination_checklist_check(cursor)

        #Save the report to file
        write_to_file()

        
if __name__ == '__main__':
    """
    Program entry point.
    Command line argument should contain a date in YYYY-MM-DD format
    """
    #making sure that date will be passed and in correct format
    if len(sys.argv) < 3:
        print "Missing date, please pass it as an argument!"
        sys.exit()
    elif not re.match(r"\d{4}-\d{2}-\d{2}", sys.argv[1]):
        print "Incorrect date format - should be YYYY-MM-DD"
        sys.exit()
    elif not re.match(r"\d{4}-\d{2}-\d{2}", sys.argv[2]):
        print "Incorrect date format - should be YYYY-MM-DD"
        sys.exit()

    runReport(sys.argv[1], sys.argv[2])
        
