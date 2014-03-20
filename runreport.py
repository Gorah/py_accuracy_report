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

    

def write_to_dict(row, ttype, notes):
    """
    This function fills dictionary with a new entry (which is another
    dictionary containing all the necessary data)
    """

    #new empty dictionary is created to store ticket data in it
    case_descr = {}
    case_descr['type'] = ttype
    #This allows overriding default notes script overriding

    
    case_descr['notes'] = notes
        
    if not row.Forname:
        forename = ' '
    else:
        forename = row.Forname
    case_descr['eename'] = row.Surname + ' ' + forename  
    case_descr['eeid'] = row.EEID
    case_descr['rootcause'] = row.CauseText

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
             T.NumberOfReminders, E.EEID, E.Forname, E.Surname, T.SourceID, R.CauseText 
             FROM tTracker as T INNER JOIN
             tMCBCEmployee as E ON T.EeID = E.ID INNER JOIN 
             tRootCause as R ON T.RootCause = R.ID 
             WHERE (T.ProcessID IN (262, 330)) AND
             (T.DateReceived BETWEEN ? AND ?) AND 
             (T.EffectiveDate < T.DateReceived OR T.CutOffDate < T.DateReceived)"""
    ttype = 'Contract Expiration - Late Renewal Submission'

    #getting recordset from DB    
    result = get_DBdata(sql, sD, eD, cursor)

    """if there are any rows in response we're checking each row to
    determine which piece of string to use in description of case.
    After string is determined row and meta data are sent to be added
    to dictionary.
    """   
    if result:
        for row in result:
            compDocs = get_compDocsString(row.CompleteDocsDate)
            docs_rec = get_compDocsString(row.CompleteDocsDate)

            notes = ('""%s%s.\n%s%s.\n%s.\n%s%s.\n%s%s.\n%s%d.\n%s.""' %
                     ('Contract End date ',
                      row.EffectiveDate.strftime('%d/%m/%Y'),
                      'PCR received on ',
                      row.DateReceived.strftime('%d/%m/%Y'),     
                      docs_rec,
                      'Request should be submitted by ',
                      row.CutOffDate.strftime('%d/%m/%Y'),
                      'Request should be submitted by ',
                      row.CutOffDate.strftime('%d/%m/%Y'),
                      'Days late for payroll cut off: ',
                      day_diff(datetime.datetime.now(), row.CutOffDate),
                      row.EEImpact
                  ))    

            write_to_dict(row, ttype, notes)                
            

def contract_no_response(sD, eD, cursor):
    """
    This function finds records where there was no response for end
    of contract reminder.
    """
    sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate, 
             T.CutOffDate, T.EEImpact, T.CompleteDocsDate, 
             T.NumberOfReminders, E.EEID, E.Forname, 
             E.Surname, T.LetterSentOn, R.CauseText 
             FROM tTracker as T INNER JOIN 
             tMCBCEmployee as E ON T.EeID = E.ID INNER JOIN 
             tRootCause as R ON T.RootCause = R.ID
             WHERE T.ProcessID IN (352, 350, 383, 399) AND 
             (T.DateReceived BETWEEN ? AND ?) AND
             (T.EffectiveDate < GETDATE() AND T.SignedLetterReceivedOn is null)
              OR (T.CutOffDate < GETDATE() AND T.SignedLetterReceivedOn is null)"""

    #getting data from DB
    result = get_DBdata(sql, sD, eD, cursor)

    if result:
        for row in result:
            notes = ('""%s%s.\n%s%s.\n%s%s.\n%s.\n%s%d.\n%s.""' %
                     ('Contract End date ',
                      row.EffectiveDate.strftime('%d/%m/%Y'),
                      'Email to manager sent on ',
                      row.LetterSentOn.strftime('%d/%m/%Y'),
                      'Request should be submitted by ',
                      row.CutOffDate.strftime('%d/%m/%Y'),
                      'Response not received from LM',
                      'Days late for payroll cut off: ',
                      day_diff(datetime.datetime.now(), row.CutOffDate),
                      row.EEImpact
                  ))
            write_to_dict(row, 'Contract Expiration - No Response', notes)
    

def contract_exp_by_letters(sD, eD, cursor):
    """
    This function fetches data for Contract Expiration category,scoping
    for the letter tickets. Late tickets are fetched and are split into
    2 different types: 'no response' and 'late submission'.
    Data is later sent to be written to dictionary
    """
    sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate, 
             T.CutOffDate, T.EEImpact, T.CompleteDocsDate, 
             T.NumberOfReminders, E.EEID, E.Forname, T.SignedLetterReceivedOn, 
             E.Surname, T.LetterSentOn, R.CauseText FROM tTracker as T INNER JOIN 
             tMCBCEmployee as E ON T.EeID = E.ID INNER JOIN
             tRootCause as R ON T.RootCause = R.ID
             WHERE T.ProcessID IN (349, 351, 352, 350, 383, 399) AND 
             (T.DateReceived BETWEEN ? AND ?) AND
             (T.SignedLetterReceivedOn < GETDATE() AND T.SignedLetterRequired = 1)
              OR (T.SignedLetterReceivedOn < GETDATE() AND T.SignedLetterRequired = 1)"""
    notes_name = 'Contract End effective date '
    
    #getting recordset from DB
    result = get_DBdata(sql, sD, eD, cursor)
    
    """if there are any rows in response we're checking each row to
    determine which piece of string to use in description of case.
    After string is determined row and meta data are sent to be added
    to dictionary.
    """
    if result:
        for row in result:
            ttype = 'Contract Expiration - Late Renewal Submission'
            notes = ('""%s%s.\n%s%s.\n%s%s.\n%s%s.\n%s%d.\n%s.""' %
                     ('Contract End date ',
                      row.EffectiveDate.strftime('%d/%m/%Y'),
                      'Email to manager sent on ',
                      row.LetterSentOn.strftime('%d/%m/%Y'),
                      'Response should be submitted by ',
                      row.CutOffDate.strftime('%d/%m/%Y'),
                      'Response from LM received on ',
                      row.SignedLetterReceivedOn.strftime('%d/%m/%Y'),
                      'Days late for payroll cut off: ',
                      day_diff(datetime.datetime.now(), row.CutOffDate),
                      row.EEImpact
                  ))    

            write_to_dict(row, ttype, notes)


def late_loa(sD, eD, cursor):
    """
    This function finds late loa cases
    """
    sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate, 
             T.EEImpact, E.EEID, E.Forname, E.Surname, P.ProcessName, T.SourceID, R.CauseText 
             FROM tTracker as T INNER JOIN
             tMCBCEmployee as E ON T.EeID = E.ID INNER JOIN
             tProcess as P ON T.ProcessID = P.ID INNER JOIN
             tRootCause as R ON T.RootCause = R.ID
             WHERE (T.ProcessID IN (246, 261, 264, 282, 284, 289, 305,
             306, 326, 341)) AND 
             (T.DateReceived BETWEEN ? AND ?)"""
    ttype = 'Leave of Absence - Late Submission'

    #getting recordset from DB
    result = get_DBdata(sql, sD, eD, cursor)

    #if there are any records in the recordset they need to be analized if
    #they are late.
    if result:
        for row in result:
            #checks if row is late. if yes adds an entry
            if check_if_late_loa(row):

                source = get_source_string(row.SourceID)
                    
                friday = row.EffectiveDate + datetime.timedelta(days=(4 - row.EffectiveDate.weekday()))
                notes = ('"%s%s.\n%s%s.\n%s%s.\n%s%s.\n%s%d.\n%s"' %
                         ('Process type: ',
                          row.ProcessName,
                          'Effective date ',
                          row.EffectiveDate.strftime('%d/%m/%Y'),
                          'Request should be submitted by ',
                          friday.strftime('%d/%m/%Y'),
                          source,
                          row.DateReceived.strftime('%d/%m/%Y'),
                          'Days late: ',
                          day_diff(row.DateReceived, friday),
                          row.EEImpact
                      ))
                write_to_dict(row, ttype, notes)


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
             T.NumberOfReminders, E.EEID, E.Forname, E.Surname, R.CauseText, T.SourceID 
             FROM tTracker as T INNER JOIN
             tMCBCEmployee as E ON T.EeID = E.ID INNER JOIN
             tRootCause as R ON T.RootCause = R.ID
             WHERE (T.ProcessID = 325) AND 
             (T.DateReceived BETWEEN ? AND ?) AND (T.EffectiveDate < T.DateReceived)"""
    ttype = 'Return from Leave - Late Submission'

    #getting recordset from DB
    result = get_DBdata(sql, sD, eD, cursor)

    #if there are any records in the recordset each row is sent to be
    #added to dictionary
    if result:
        for row in result:
            source = get_source_string(row.SourceID)

            #make sure to use a date. If complete docs la    
            compDocs = get_compDocsString(row.CompleteDocsDate)
            dateRec = get_docsDate(row.CompleteDocsDate)
                
            if (row.DateReceived - row.EffectiveDate).days > 0:
                notes = ('"%s%s.\n%s%s.\n%s.\n%s%s.\n%s%d.\n%s"' %('Return effective on ',
                                                                       row.EffectiveDate.strftime('%d/%m/%Y'),
                                                                       source,
                                                                       row.DateReceived.strftime('%d/%m/%Y'),
                                                                       compDocs,     
                                                                       'Request should be submitted by ',
                                                                       row.EffectiveDate.strftime('%d/%m/%Y'),
                                                                       'Days late for payroll cut off: ',
                                                                       day_diff(docsDate, row.EffectiveDate),
                                                                       row.EEImpact
                                                                   ))
                write_to_dict(row, ttype, notes)

    

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
            notes_override = ('"%s%s.\n%s%s.\n%s%s.\n%s%d.\n%s"' %(notes_name,
                                                                   row.EffectiveDate.strftime('%d/%m/%Y'),
                                                                   'Complete info received on ',
                                                                   row.DocsReceivedDate.strftime('%d/%m/%Y'),
                                                                   'Request should be submitted by ',
                                                                   row.CutOffDate.strftime('%d/%m/%Y'),
                                                                   'Days late for payroll cut off: ',
                                                                   day_diff(row.DocsReceivedDate, row.CutOffDate),
                                                                   row.EEImpact))
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


def late_jobchange_action(sD, eD, cursor):
    """
    This function finds late job change actions in SAP among tickets
    """
    sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate,
             T.CutOffDate, T.EEImpact, T.CompleteDocsDate,
             T.NumberOfReminders, E.EEID, E.Forname, E.Surname , R.CauseText, T.SourceID 
             FROM tTracker as T INNER JOIN
             tMCBCEmployee as E ON T.EeID = E.ID INNER JOIN
             tRootCause as R ON T.RootCause = R.ID
             WHERE (T.ProcessID IN (315, 331, 323, 335, 340, 339) AND 
             T.DateReceived BETWEEN ? AND ?) AND  
             (((T.EffectiveDate < T.CompleteDocsDate) OR
             (T.CutOffDate < T.CompleteDocsDate) AND T.CompleteDocsDate IS NOT NULL)
             OR ((T.EffectiveDate < T.DateReceived OR T.CutOffDate < T.DateReceived) AND
             T.CompleteDocsDate IS NULL))"""
    ttype = "Job Change - Late Submission"

    #getting recordset from DB
    result = get_DBdata(sql, sD, eD, cursor)

    
    if result:
        for row in result:
            source = get_source_string(row.SourceID)
            compDocs = get_compDocsString(row.CompleteDocsDate)
            dateRec = get_docsDate(row.CompleteDocsDate)

            notes = ('"%s%s.\n%s%s.\n%s.\n%s%s.\n%s%d.\n%s"' %
                     ('Job change effective on ',
                      row.EffectiveDate.strftime('%d/%m/%Y'),
                      source,
                      row.DateReceived.strftime('%d/%m/%Y'),
                      compDocs,
                      'Request should be submitted by ',
                      row.CutOffDate.strftime('%d/%m/%Y'),
                      'Days late for payroll cut off: ',
                      day_diff(dateRec, row.CutOffDate),
                      row.EEImpact
                  ))
            write_to_dict(row, ttype, notes)   


def late_jobchange_letters(sD, eD, cursor):
    """
    This function finds late job change letters
    """
    sql = """SELECT T.ID, T.DateReceived, T.CompleteDocsDate, T.EffectiveDate, 
             T.CutOffDate, T.EEImpact, T.SignedLetterReceivedOn, 
             T.NumberOfReminders, E.EEID, E.Forname, 
             E.Surname, T.LetterReceived, T.SignedLetterRequired, 
             T.LetterSentOn, R.CauseText, T.SourceID  
             FROM tTracker as T INNER JOIN 
             tMCBCEmployee as E ON T.EeID = E.ID INNER JOIN
             tRootCause as R ON T.RootCause = R.ID
             WHERE (T.ProcessID IN (363, 385, 386, 400, 410, 412, 413)) AND
             (T.DateReceived BETWEEN ? AND ?) AND 
             ((T.EffectiveDate < T.CompleteDocsDate) OR
             (T.CutOffDate < T.CompleteDocsDate) OR 
             (T.EffectiveDate < T.SignedLetterReceivedOn AND T.SignedLetterRequired = 1
             AND T.SignedLetterReceivedOn IS NOT NULL) OR 
             (T.CutOffDate < T.SignedLetterReceivedOn AND T.SignedLetterRequired = 1
             AND T.SignedLetterReceivedOn IS NOT NULL) OR
             (T.SignedLetterRequired = 1 AND T.SignedLetterReceivedOn IS NULL AND 
             T.EffectiveDate < GETDATE()) OR
             (T.SignedLetterRequired = 1 AND T.SignedLetterReceivedOn IS NULL AND 
             T.CutOffDate < GETDATE()))"""
    ttype = "Job Change - Late Submission"

    #grab recordset from DB
    result = get_DBdata(sql, sD, eD, cursor)

    if result:
        for row in result:
            source = get_source_string(row.SourceID)
            compDocs = get_compDocsString(row.CompleteDocsDate)
            dateRec = get_docsDate(row.CompleteDocsDate)
            
            #create statuses of signed letter received back
            #basing on date conditions
            if row.LetterReceived == 1 and  row.SignedLetterReceivedOn:
                sigLetter = ('"%s%s.\n"' % ('Signed letter received on ',
                                            row.SignedLetterReceivedOn.strftime('%d/%m/%Y')))
            elif row.LetterReceived == 1 and row.SignedLetterRequired == 1 and not row.SignedLetterReceivedOn:
                sigLetter = '"Signed letter not yet returned.\n"'
            elif row.LetterReceived == 0:
                sigLetter = ''
                
            #create statuses for  letter sent, offer pack sent based on dates    
            if row.LetterReceived == 1:
                letterSent = ('s%s%' % ('Letter sent on ',
                                        row.LetterSentOn.strftime('%d/%m/%Y')))
            else:
                letterSent = 'Letter not sent yet'
            
            #calculate amount of days late basing on currenn document and contract statuses
            #and on docs submission date
            if row.CompleteDocsDate > row.CutOffDate:
                days = day_diff(row.CutOffDate, row.CompleteDocsDate)
            elif row.CompleteDocsDate > row.EffectiveDate:
                days = day_diff(row.EffectiveDate, row.CompleteDocsDate)
                
            if row.SignedLetterReceivedOn > row.CutOffDate:
                days = day_diff(row.SignedLetterReceivedOn, row.CutOffDate)
            elif row.SignedLetterReceivedOn > row.EffectiveDate:
                days = day_diff(row.SignedLetterReceivedOn, row.EffectiveDate)

            #create notes field
            notes = ('"%s%s.\n%s%s.\n%s.\n%s.\n%s%s%s.\n%s%d.\n%s."' %
                     ('Job change effective on ',
                      row.EffectiveDate.strftime('%d/%m/%Y'),
                      source,
                      row.DateReceived.strftime('%d/%m/%Y'),
                      compDocs,
                      letterSent,
                      sigLetter,
                      'Request should be submitted by ',
                      row.CutOffDate.strftime('%d/%m/%Y'),
                      'Days late for payroll cut off: ',
                      days,
                      row.EEImpact
                  ))

            write_to_dict(row, ttype, notes)


def late_paychange_action(sD, eD, cursor):
    """
    This function finds late job change actions in SAP among tickets
    """
    sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate,
             T.CutOffDate, T.EEImpact, T.CompleteDocsDate,
             T.NumberOfReminders, E.EEID, E.Forname, E.Surname , R.CauseText, T.SourceID 
             FROM tTracker as T INNER JOIN
             tMCBCEmployee as E ON T.EeID = E.ID INNER JOIN
             tRootCause as R ON T.RootCause = R.ID
             WHERE (T.ProcessID IN (327, 328, 329) AND 
             T.DateReceived BETWEEN ? AND ?) AND  
             (((T.EffectiveDate < T.CompleteDocsDate) OR
             (T.CutOffDate < T.CompleteDocsDate) AND T.CompleteDocsDate IS NOT NULL)
             OR ((T.EffectiveDate < T.DateReceived OR T.CutOffDate < T.DateReceived) AND
             T.CompleteDocsDate IS NULL))"""
    ttype = "Pay Change - Late Submission"

    #getting recordset from DB
    result = get_DBdata(sql, sD, eD, cursor)

    
    if result:
        for row in result:
            source = get_source_string(row.SourceID)
            compDocs = get_compDocsString(row.CompleteDocsDate)
            dateRec = get_docsDate(row.CompleteDocsDate)

            notes = ('"%s%s.\n%s%s.\n%s.\n%s%s.\n%s%d.\n%s"' %
                     ('Pay change effective on ',
                      row.EffectiveDate.strftime('%d/%m/%Y'),
                      source,
                      row.DateReceived.strftime('%d/%m/%Y'),
                      compDocs,
                      'Request should be submitted by ',
                      row.CutOffDate.strftime('%d/%m/%Y'),
                      'Days late for payroll cut off: ',
                      day_diff(dateRec, row.CutOffDate),
                      row.EEImpact
                  ))
            write_to_dict(row, ttype, notes)               
            

def late_paychange_letters(sD, eD, cursor):
    """
    This function finds late job change letters
    """
    sql = """SELECT T.ID, T.DateReceived, T.CompleteDocsDate, T.EffectiveDate, 
             T.CutOffDate, T.EEImpact, T.SignedLetterReceivedOn, 
             T.NumberOfReminders, E.EEID, E.Forname, 
             E.Surname, T.LetterReceived, T.SignedLetterRequired, 
             T.LetterSentOn, R.CauseText, T.SourceID  
             FROM tTracker as T INNER JOIN 
             tMCBCEmployee as E ON T.EeID = E.ID INNER JOIN
             tRootCause as R ON T.RootCause = R.ID
             WHERE (T.ProcessID IN (363, 385, 386, 400, 410, 412, 413)) AND
             (T.DateReceived BETWEEN ? AND ?) AND 
             ((T.EffectiveDate < T.CompleteDocsDate) OR
             (T.CutOffDate < T.CompleteDocsDate) OR 
             (T.EffectiveDate < T.SignedLetterReceivedOn AND T.SignedLetterRequired = 1
             AND T.SignedLetterReceivedOn IS NOT NULL) OR 
             (T.CutOffDate < T.SignedLetterReceivedOn AND T.SignedLetterRequired = 1
             AND T.SignedLetterReceivedOn IS NOT NULL) OR
             (T.SignedLetterRequired = 1 AND T.SignedLetterReceivedOn IS NULL AND 
             T.EffectiveDate < GETDATE()) OR
             (T.SignedLetterRequired = 1 AND T.SignedLetterReceivedOn IS NULL AND 
             T.CutOffDate < GETDATE()))"""
    ttype = "Pay Change - Late Submission"

    #grab recordset from DB
    result = get_DBdata(sql, sD, eD, cursor)

    if result:
        for row in result:
            source = get_source_string(row.SourceID)
            compDocs = get_compDocsString(row.CompleteDocsDate)
            dateRec = get_docsDate(row.CompleteDocsDate)
            
            #create statuses of signed letter received back
            #basing on date conditions
            if row.LetterReceived == 1 and  row.SignedLetterReceivedOn:
                sigLetter = ('"%s%s.\n"' % ('Signed letter received on ',
                                             row.SignedLetterReceivedOn.strftime('%d/%m/%Y')))
            elif row.LetterReceived == 1 and row.SignedLetterRequired == 1 and not row.SignedLetterReceivedOn:
                sigLetter = '"Signed letter not yet returned.\n"'
            elif row.LetterReceived == 0:
                sigLetter = ''
                
            #create statuses for  letter sent, offer pack sent based on dates    
            if row.LetterReceived == 1:
                letterSent = ('s%s%' % ('Letter sent on ',
                                        row.LetterSentOn.strftime('%d/%m/%Y')))
            else:
                letterSent = 'Letter not sent yet'
                
            #calculate amount of days late basing on currenn document and contract statuses
            #and on docs submission date
            if row.CompleteDocsDate > row.CutOffDate:
                days = day_diff(row.CutOffDate, row.CompleteDocsDate)
            elif row.CompleteDocsDate > row.EffectiveDate:
                days = day_diff(row.EffectiveDate, row.CompleteDocsDate)
                
            if row.SignedLetterReceivedOn > row.CutOffDate:
                days = day_diff(row.SignedLetterReceivedOn, row.CutOffDate)
            elif row.SignedLetterReceivedOn > row.EffectiveDate:
                days = day_diff(row.SignedLetterReceivedOn, row.EffectiveDate)

            #create notes field
            notes = ('"%s%s.\n%s%s.\n%s.\n%s.\n%s%s%s.\n%s%d.\n%s."' %
                     ('Pay change effective on ',
                      row.EffectiveDate.strftime('%d/%m/%Y'),
                      source,
                      row.DateReceived.strftime('%d/%m/%Y'),
                      compDocs,
                      letterSent,
                      sigLetter,
                      'Request should be submitted by ',
                      row.CutOffDate.strftime('%d/%m/%Y'),
                      'Days late for payroll cut off: ',
                      days,
                      row.EEImpact
                  ))

            write_to_dict(row, ttype, notes)

            
def late_hire(sD, eD, cursor):
    """
    This function finds late hire actions
    """
    sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate,
             T.CutOffDate, T.EEImpact, T.CompleteDocsDate,
             T.NumberOfReminders, E.EEID, E.Forname, E.Surname , T.LetterReceived,
             T.LetterSentOn, T.SignedLetterReceivedOn, T.CloseDate, R.CauseText
             FROM tTracker as T INNER JOIN
             tMCBCEmployee as E ON T.EeID = E.ID INNER JOIN
             tRootCause as R ON T.RootCause = R.ID
             WHERE (T.ProcessID IN (371, 372) AND 
            (T.DateReceived BETWEEN ? AND ?)) AND 
            ((T.EffectiveDate < T.DateReceived OR T.CutOffDate < T.DateReceived
            AND T.CompleteDocsDate IS NULL) OR (T.SignedLetterReceivedOn > T.EffectiveDate) 
            OR (T.SignedLetterReceivedOn > T.CutOffDate) OR (T.CompleteDocsDate > T.EffectiveDate 
            OR T.CompleteDocsDate > T.CutOffDate) OR 
            (T.SignedLetterReceivedOn IS NULL AND (T.CutOffDate < GETDATE() OR 
	    T.EffectiveDate < GETDATE())))"""

    result = get_DBdata(sql, sD, eD, cursor)
    ttype = 'Hires - Missing Documentation'
    
    if result:
        for row in result:
            # if complete documents date is set use it as Complete docs received on
            # else note that complete docs were not received yet
            compDocs = get_compDocsString(row.CompleteDocsDate)

            #create statuses of signed letter received back
            #basing on date conditions
            if row.LetterReceived == 1 and  row.SignedLetterReceivedOn:
                sigLetter = ('"%s%s.\n"' % ('Signed contract received on ',
                                       row.SignedLetterReceivedOn.strftime('%d/%m/%Y')))
            elif row.LetterReceived == 1 and not row.SignedLetterReceivedOn:
                sigLetter = '"Signed contract not yet returned.\n"'
            elif row.LetterReceived == 0:
                sigLetter = ''

            #create statuses for  letter sent, offer pack sent based on dates    
            if row.LetterReceived == 1:
                letterSent = ('s%s%' % ('Contract sent on ',
                                        row.LetterSentOn.strftime('%d/%m/%Y')))
                offPack = ('s%s%' % ('Offer pack sent on ',
                                     row.CloseDate.strftime('%d/%m/%Y')))
            else:
                letterSent = 'Contract not sent yet'
                offPack = 'Offer pack not sent yet'

            #calculate amount of days late basing on currenn document and contract statuses
            #and on docs submission date
            if row.CompleteDocsDate > row.CutOffDate:
                days = day_diff(row.CutOffDate, row.CompleteDocsDate)
            elif row.CompleteDocsDate > row.EffectiveDate:
                days = day_diff(row.EffectiveDate, row.CompleteDocsDate)
                
            if row.SignedLetterReceivedOn > row.CutOffDate:
                days = day_diff(row.SignedLetterReceivedOn, row.CutOffDate)
            elif row.SignedLetterReceivedOn > row.EffectiveDate:
                days = day_diff(row.SignedLetterReceivedOn, row.EffectiveDate)

            #create notes string    
            notes = ('"%s%s.\n%s%s.\n%s.\n%s%s%s.\n%s%d.\n%s"' %('New Hire effective on ',
                                                                 row.EffectiveDate.strftime('%d/%m/%Y'),
                                                                 compDocs,
                                                                 LetterSent,
                                                                 offPack,
                                                                 sigLetter,
                                                                 'Request should be submitted by ',
                                                                 row.CutOffDate.strftime('%d/%m/%Y'),
                                                                 'Days late: ',
                                                                 days,
                                                                 row.EEImpact))
            #write result to dictionary
            write_to_dict(row, ttype, notes)
                

def late_termination(sD, eD, cursor):
    """
    This function finds late job change actions in SAP among tickets
    """
    sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate,
             T.CutOffDate, T.EEImpact, T.CompleteDocsDate,
             T.NumberOfReminders, E.EEID, E.Forname, E.Surname , R.CauseText, T.SourceID 
             FROM tTracker as T INNER JOIN
             tMCBCEmployee as E ON T.EeID = E.ID INNER JOIN
             tRootCause as R ON T.RootCause = R.ID
             WHERE (T.ProcessID IN (327, 328, 329) AND 
             T.DateReceived BETWEEN ? AND ?) AND  
             (((T.EffectiveDate < T.CompleteDocsDate) OR
             (T.CutOffDate < T.CompleteDocsDate) AND T.CompleteDocsDate IS NOT NULL)
             OR ((T.EffectiveDate < T.DateReceived OR T.CutOffDate < T.DateReceived) AND
             T.CompleteDocsDate IS NULL))"""
    ttype = "Termination - Late Submission"

    #getting recordset from DB
    result = get_DBdata(sql, sD, eD, cursor)

    
    if result:
        for row in result:
            source = get_source_string(row.SourceID)
            compDocs = get_compDocsString(row.CompleteDocsDate)
            dateRec = get_docsDate(row.CompleteDocsDate)

            notes = ('"%s%s.\n%s%s.\n%s.\n%s%s.\n%s%d.\n%s"' %
                     ('Termination effective on ',
                      row.EffectiveDate.strftime('%d/%m/%Y'),
                      source,
                      row.DateReceived.strftime('%d/%m/%Y'),
                      compDocs,
                      'Request should be submitted by ',
                      row.CutOffDate.strftime('%d/%m/%Y'),
                      'Days late for payroll cut off: ',
                      day_diff(dateRec, row.CutOffDate),
                      row.EEImpact
                  ))
            write_to_dict(row, ttype, notes)

            
def termination_complete_docs(sD, eD, cursor):
    """
    This generic function collects data about specified category and
    sends records with late tickets to be added to dictionary.

    It takes into consideration action in SAP tickets complete
    documentation received.
    """
    sql = """SELECT T.ID, T.DateReceived, T.EffectiveDate,
             T.CutOffDate, T.EEImpact, T.CompleteDocsDate,
             T.NumberOfReminders, E.EEID, E.Forname, E.Surname, R.CauseText 
             FROM tTracker as T LEFT JOIN
             tMCBCEmployee as E ON T.EeID = E.ID INNER JOIN 
             tRootCause as R ON T.RootCause = R.ID
             WHERE T.ProcessID IN (336, 337, 338) AND 
             (T.DateReceived BETWEEN ? AND ?) 
             AND (T.EffectiveDate <= T.DateReceived OR
             (T.CutOffDate < T.DateReceived)) AND T.CompleteDocsDate
             IS NOT NULL"""
    notes_name = 'Termination effective '
    ttype = 'Termination - Late Submission'
    notes_override = None

    #getting recordset from DB
    result = get_DBdata(sql, sD, eD, cursor)

    #if there are any records in the recordset each row is sent to be
    #added to dictionary
    if result:
        for row in result:
            docs_rec = '.\nComplete details received on '
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
             T.NumberOfReminders, E.EEID, E.Forname, E.Surname, R.CauseText 
             FROM tTracker as T LEFT JOIN 
             tMCBCEmployee as E ON T.EeID = E.ID INNER JOIN
             tRootCause as R ON T.RootCause = R.ID
             WHERE T.ProcessID IN (336, 337, 338) AND 
             (T.DateReceived BETWEEN ? AND ?) 
             AND (T.EffectiveDate <= T.DateReceived OR
             (T.CutOffDate < T.DateReceived)) AND T.CompleteDocsDate
             IS NULL"""
    notes_name = 'Termination effective '
    ttype = 'Termination - Late Submission'
    docs_rec = '.\nComplete details still pending'
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
             E.Surname, T.LetterReceived, R.CauseText 
             FROM tTracker as T INNER JOIN 
             tMCBCEmployee as E ON T.EeID = E.ID INNER JOIN 
             tRootCause as R ON T.RootCause = R.ID
             WHERE (T.ProcessID = 417) AND (T.LetterReceived = 0)"""
    ttype = 'Termination -  No Termination Checklist submitted'
    
    #getting recordset from DB
    sD = None
    eD = None
    result = get_DBdata(sql, sD, eD, cursor)

    #if there are any records in the recordset each row is sent to be
    #added to dictionary
    if result:
        for row in result:
            notes = ('Possible SOX audit compliance issue')
            write_to_dict(row, ttype, notes)


def get_source_string(sourceID):
     if sourceID == 2:
         return 'PCR received on '
     else:
         return 'Non-PCR request received on'

         
def get_docsDate(compdate):
    if compdate:
        return compdate
    else:
        return datetime.datetime.today()
        
def get_compDocsString(compdate):
    if compdate:
        return ('%s%s' % ('Complete request received on ',
                                      row.CompleteDocsDate.strftime('%d/%m/%Y')))
    else:
        return 'Complete documents still pending'

        
def write_to_file():
    """
    This function saves report to csv file
    """
    
    #Open file to save report to
    report = open('report.csv', 'w')
    for key in LATE_CASES:
        #build file entry row from dict data
        
        fileentry = '%d,%s,%s,%s,%s,%d' % (key, LATE_CASES[key]['type'],
                                           LATE_CASES[key]['notes'],
                                           LATE_CASES[key]['rootcause'],
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
        contract_no_response(sD, eD, cursor)
        #LOA section
        late_loa(sD, eD, cursor)
        #Return From LOA section
        ret_from_loa_by_dates(sD, eD, cursor)

        #Job Change section
        #Job Changes action tickets
        late_jobchange_action(sD, eD, cursor)
        #Job Changes letter tickets
        late_jobchange_letters(sD, eD, cursor)

        #New Hire section
        late_hire(sD, eD, cursor)

        #Pay Changes section
        procname = 'Pay Change'
        #Pay Changes action tickets
        scope = '327, 328, 329'
        late_paychange_action(sD, eD, cursor)
        #Pay Changes letter tickets
        procname = 'Pay Change - Late Submission'
        scope = '395, 396, 397, 347'
        late_paychange_letters(sD, eD, cursor)


        #Termination section
        procname = 'Termination'
        #Termination actions
        late_termination(sD, eD, cursor)
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
        
