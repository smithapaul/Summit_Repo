DROP PROCEDURE DLMRT_OWNER."CensusCopy"
/

--
-- "CensusCopy"  (Procedure) 
--
CREATE OR REPLACE PROCEDURE DLMRT_OWNER."CensusCopy"
        (
                i_MartId                in  Varchar2    Default 'DlabCpy',
                i_ProcessName           in  Varchar2    Default 'CensusCopy',
                i_SourceDatabaseName    in  Varchar2    Default 'DWPRD11',
                i_Institution           in  Varchar2,
                i_CensusPeriod          in  Varchar2,
                i_CensusSequence        in  Varchar2
        )
AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Copies the content of Data Lab tables from another database's version of the tables.
-- The entire contents of the target table is replaced.
-- Restrictions:
-- o Tables are hard-coded in recTable's select statement.
-- o Tables must be in schema DLMRT_OWNER.
-- o If table is partitioned it must be list partitioned by INSTITUTION, CENSUS_PERIOD, CENSUS_SEQ.
--
--
-- V03                   07/26/2021     Srikanth
--                                      updated the case statement for strSourceDbLink to reflected new 18c database names
-- V02  CASE-32059      05/21/2020      Greg Kampf
--                                      Enhance to selectively copy censuses.
--
-- V01  CASE-18513      03/09/2020      Greg Kampf
--
------------------------------------------------------------------------

        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(32767);
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) := '';
        strSqlDynamic                   Varchar2(32767) := '';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;
        intYear                         Integer;
        intNumberOfPriorYears           Integer         := 6;
        intFiscalYearCurrent            Integer;
        intFiscalYearStart              Integer;
        dtPeriodEndDateStart            Date;
        strDataTimestampRowExists       Varchar2(1);
        strFileName                     Varchar2(256);
        dtFileAsOfTime                  Date;
        strDateTimeMask                 Varchar2(22)    := 'DD-MON-YYYY HH24:MI:SS';
        dtSourceDate                    Date;
        strTargetDatabaseName           Varchar2(128);
        strSourceDatabaseName           Varchar2(128);
        strSourceDbLink                 Varchar2(128);
        strPartitionName                Varchar2(128);
        strPartitionValuesList          Varchar2(200);
        strColumnList                   Varchar2(32767);
        intTableRefreshCount            Integer         := 0;
        strQuote                        Varchar2(1)     := chr(39);
        strPartitioned                  Varchar2(3);
        strHint                         Varchar2(100);
        strOrderBy                      Varchar2(200);
        intCensusCount                  Integer         := 0;

BEGIN
strSqlCommand   := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (i_ProcessName);

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => i_MartId,
                i_ProcessName           => i_ProcessName,
                i_ProcessStartTime      => dtProcessStart
        );

strMessage01    := 'Procedure DLMRT_OWNER."' || i_ProcessName || '" arguments:'
                || strNewLine || '             i_MartId: ' || i_MartId
                || strNewLine || ' i_SourceDatabaseName: ' || Case when i_SourceDatabaseName is null Then '{null}' Else i_SourceDatabaseName End
                || strNewLine || '        i_Institution: ' || i_Institution
                || strNewLine || '       i_CensusPeriod: ' || i_CensusPeriod
                || strNewLine || '     i_CensusSequence: ' || i_CensusSequence;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'SELECT strDbName';
SELECT  UPPER(SYS_CONTEXT('USERENV','DB_NAME')) DB_NAME
  INTO  strTargetDatabaseName
  FROM  DUAL;

strSourceDatabaseName   := upper(i_SourceDatabaseName);

strMessage01    := 'Source database: ' || strSourceDatabaseName || ',  Target database: ' || strTargetDatabaseName;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'Validate databases';
If  strTargetDatabaseName = strSourceDatabaseName
Then
        RAISE_APPLICATION_ERROR( -20001, 'Source and target databases (' || strSourceDatabaseName || ' and ' || strTargetDatabaseName || ')  are the same.  Source and target databases must be different.');
End If;

strSourceDbLink := Case
                        When    i_SourceDatabaseName = 'DWDEV11'
                        Then    'SMTDEV'
                        When    i_SourceDatabaseName = 'DWTST11'
                        Then    'SMTTEST'
                        When    i_SourceDatabaseName = 'DWUAT11'
                        Then    'SMTUAT'
                        When    i_SourceDatabaseName = 'DWPRD11'
                        Then    'SMTPROD'
                        Else    Null
                   End;

strSqlCommand  := 'Validate dblink';
If  strSourceDbLink is Null
Then
        RAISE_APPLICATION_ERROR( -20001, 'Unable to determine source dblink.');
Else
        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => 'strSourceDbLink: ' || strSourceDbLink);
End If;


strSqlDynamic   := 'TRUNCATE TABLE DLSTG_OWNER.CENSUS_COPY_S1';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
        (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                i_ShowStatement         => True,
                i_ShowSuccess           => False,
                o_Tries                 => intTries
        );

strSqlCommand   := 'Build insert statement (CENSUS_COPY_S1)';
strSqlDynamic   := '
INSERT  /*+APPEND*/
  INTO  DLSTG_OWNER.CENSUS_COPY_S1
        (
        INSTITUTION,
        CENSUS_PERIOD,
        CENSUS_SEQ,
        INSERT_TIME
        )
SELECT  DISTINCT
        SRC.INSTITUTION,
        SRC.CENSUS_PERIOD,
        SRC.CENSUS_SEQ,
        SYSDATE INSERT_TIME
  FROM  DLMRT_OWNER.CENSUS_STATUS@' || strSourceDbLink || ' SRC
 WHERE  SRC.INSTITUTION         LIKE ''' || i_Institution    || '''
   AND  SRC.CENSUS_PERIOD       LIKE ''' || i_CensusPeriod   || '''
   AND  TO_CHAR(SRC.CENSUS_SEQ) LIKE ''' || i_CensusSequence || '''
ORDER BY
        1, 2, 3'
;

strMessage01    := 'Inserting CENSUS_COPY_S1 rows...';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: INSERT INTO DLSTG_OWNER.CENSUS_COPY_S1';
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
        (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                i_ShowStatement         => True,
                i_ShowSuccess           => True,
                i_RollbackOnRetry       => True,
                o_Tries                 => intTries
        );

intRowCount     := to_number(COMMON_OWNER.SMT_CONTEXT.GET_ATTRIBUTE(i_AttributeName => 'RowCount'));

strSqlCommand   := 'COMMIT';
COMMIT;

strMessage01    := '# of rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName       => 'CENSUS_COPY_S1',
                i_Action                => 'INSERT',
                i_RowCount              => intRowCount
        );

strSqlCommand   := 'For recCensus';
For recCensus in
        (
        SELECT  DISTINCT
                CPY.INSTITUTION,
                CPY.CENSUS_PERIOD,
                CPY.CENSUS_SEQ,
                SYSDATE INSERT_TIME
          FROM  DLSTG_OWNER.CENSUS_COPY_S1 CPY
        ORDER BY
                1, 2, 3, 4
        )
Loop
        strMessage01    := 'Processing census ' || recCensus.INSTITUTION || '/' || recCensus.CENSUS_PERIOD || '/' || TO_CHAR(recCensus.CENSUS_SEQ) || '...';
        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

        strSqlCommand   := 'For recTable';
        For recTable in
                (
                SELECT  'CENSUS_STATUS'                 TABLE_NAME
                  FROM  DUAL
                UNION ALL
                SELECT  'IR_PERSON_PRELIM'              TABLE_NAME
                  FROM  DUAL
                UNION ALL
                SELECT  'IR_PERSON_CONFIRMED'           TABLE_NAME
                  FROM  DUAL
                UNION ALL
                SELECT  'IR_CITIZENSHIP_DTL_PRELIM'     TABLE_NAME
                  FROM  DUAL
                UNION ALL
                SELECT  'IR_CITIZENSHIP_DTL_CONFIRMED'  TABLE_NAME
                  FROM  DUAL
                UNION ALL
                SELECT  'IR_STUDENT_ENROL_PRELIM'       TABLE_NAME
                  FROM  DUAL
                UNION ALL
                SELECT  'IR_STUDENT_ENROL_CONFIRMED'    TABLE_NAME
                  FROM  DUAL
                )
        Loop
                strClientInfo   := i_ProcessName || ' (' || recCensus.INSTITUTION || '/' || recCensus.CENSUS_PERIOD || '/' || TO_CHAR(recCensus.CENSUS_SEQ) || ' - ' || recTable.TABLE_NAME || ')';
                strSqlCommand   := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
                DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strClientInfo);

                strMessage01    := 'Processing table ' || recTable.TABLE_NAME;
                COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


                strSqlCommand   := 'SELECT INTO strPartitioned';
                SELECT  (
                        SELECT  TAB.PARTITIONED
                          FROM  ALL_TABLES TAB
                         WHERE  TAB.OWNER       = 'DLMRT_OWNER'
                           AND  TAB.TABLE_NAME  = recTable.TABLE_NAME
                        ) PARTITIONED
                  INTO  strPartitioned
                  FROM  DUAL
                ;

                strMessage01    := 'strPartitioned: ' || strPartitioned;
                COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
                
                If  strPartitioned = 'NO'
                Then
                        strSqlCommand   := 'SMT_INDEX.ALL_UNUSABLE (non-partitioned)';
                        COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE
                                (
                                        i_TableOwner                    => 'DLMRT_OWNER',
                                        i_TableName                     => recTable.TABLE_NAME,
                                        i_IncludeJoinedTables           => True,
                                        i_IncludePartitionedIndexes     => True,
                                        i_PartitionName                 => Null,
                                        i_BitmapsOnly                   => True,
                                        i_DetailLogging                 => False
                                );

                        strSqlCommand   := 'Build target table delete statement (' || recTable.TABLE_NAME || ')';
                        strSqlDynamic   := '
                        DELETE
                          FROM  DLMRT_OWNER.' || recTable.TABLE_NAME || ' TGT
                         WHERE  TGT.INSTITUTION         = ''' || recCensus.INSTITUTION    || '''
                           AND  TGT.CENSUS_PERIOD       = ''' || recCensus.CENSUS_PERIOD  || '''
                           AND  TGT.CENSUS_SEQ          = ' || recCensus.CENSUS_SEQ
                        ;

                        strMessage01    := 'Deleting ' || recTable.TABLE_NAME || ' rows...';
                        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

                        strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: Delete target table rows';
                        COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                                (
                                        i_SqlStatement          => strSqlDynamic,
                                        i_MaxTries              => 10,
                                        i_WaitSeconds           => 10,
                                        i_ShowStatement         => True,
                                        i_ShowSuccess           => True,
                                        i_RollbackOnRetry       => True,
                                        o_Tries                 => intTries
                                );

                        intRowCount     := to_number(COMMON_OWNER.SMT_CONTEXT.GET_ATTRIBUTE(i_AttributeName => 'RowCount'));

                        strSqlCommand   := 'COMMIT';
                        COMMIT;

                        strMessage01    := '# of rows deleted: ' || TO_CHAR(intRowCount,'999,999,999,999');
                        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

                        strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
                        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
                                (
                                        i_TargetTableName       => recTable.TABLE_NAME,
                                        i_Action                => 'DELETE',
                                        i_RowCount              => intRowCount
                                );
                Else
                        strPartitionName:= recCensus.INSTITUTION || '_' || recCensus.CENSUS_PERIOD || '_' || TO_CHAR(recCensus.CENSUS_SEQ);
                        strPartitionValuesList  := strQuote || recCensus.INSTITUTION || strQuote || ',' || strQuote || recCensus.CENSUS_PERIOD || strQuote || ',' || to_char(recCensus.CENSUS_SEQ);
                        strSqlCommand   := 'SMTCMN_PART.LIST_PARTITION_CREATE';
                        COMMON_OWNER.SMTCMN_PART.LIST_PARTITION_CREATE
                                (       i_TableOwner                    => 'DLMRT_OWNER',
                                        i_TableName                     => recTable.TABLE_NAME,
                                        i_PartitionName                 => strPartitionName,
                                        i_TestMode                      => False,
                                        i_PartitionValuesList           => strPartitionValuesList
                                );
                        strSqlDynamic   := 'ALTER TABLE DLMRT_OWNER.' || recTable.TABLE_NAME || ' TRUNCATE PARTITION ' || strPartitionName || ' UPDATE INDEXES';
                        strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
                        COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                                (
                                        i_SqlStatement          => strSqlDynamic,
                                        i_MaxTries              => 10,
                                        i_WaitSeconds           => 10,
                                        i_ShowStatement         => True,
                                        i_ShowSuccess           => False,
                                        o_Tries                 => intTries
                                );
                        strSqlCommand   := 'SMT_INDEX.ALL_UNUSABLE (partition)';
                        COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE
                                (
                                        i_TableOwner                    => 'DLMRT_OWNER',
                                        i_TableName                     => recTable.TABLE_NAME,
                                        i_IncludeJoinedTables           => True,
                                        i_IncludePartitionedIndexes     => True,
                                        i_PartitionName                 => strPartitionName,
                                        i_BitmapsOnly                   => True,
                                        i_DetailLogging                 => False
                                );
                End If;

                -- Build column list
                strSqlCommand   := 'SMTCMN_PART.LIST_PARTITION_CREATE';
                COMMON_OWNER.SMTCMN_PART.LIST_PARTITION_CREATE
                        (       i_TableOwner                    => 'DLSTG_OWNER',
                                i_TableName                     => 'COLUMN_REFRESH_S1',
                                i_PartitionName                 => recTable.TABLE_NAME,
                                i_TestMode                      => False,
                                i_PartitionValuesList           => strQuote || recTable.TABLE_NAME || strQuote
                        );

                strSqlDynamic   := 'ALTER TABLE DLSTG_OWNER.COLUMN_REFRESH_S1 TRUNCATE PARTITION ' || recTable.TABLE_NAME || ' UPDATE INDEXES';
                strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
                COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                        (
                                i_SqlStatement          => strSqlDynamic,
                                i_MaxTries              => 10,
                                i_WaitSeconds           => 10,
                                i_ShowStatement         => True,
                                i_ShowSuccess           => False,
                                o_Tries                 => intTries
                        );

                strSqlCommand   := 'Build insert statement (COLUMN_REFRESH_S1)';
                strSqlDynamic   := '
                INSERT  /*+APPEND*/
                  INTO  DLSTG_OWNER.COLUMN_REFRESH_S1
                        (
                        TABLE_NAME,
                        COLUMN_NAME,
                        COLUMN_ID,
                        INSERT_TIME
                        )
                SELECT  DISTINCT
                        ' || strQuote || recTable.TABLE_NAME || strQuote || ' TABLE_NAME,
                        SRC.COLUMN_NAME,
                        SRC.COLUMN_ID,
                        SYSDATE                         INSERT_TIME
                  FROM  ALL_TAB_COLS ' || '@' || strSourceDbLink || ' SRC
                 WHERE  OWNER           = ''DLMRT_OWNER''
                   AND  TABLE_NAME      = ' || strQuote || recTable.TABLE_NAME || strQuote || '
                   AND  VIRTUAL_COLUMN  = ''NO''
                ORDER BY
                        2, 3'
                ;

                strMessage01    := 'Inserting COLUMN_REFRESH_S1 rows...';
                COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

                strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: INSERT INTO DLSTG_OWNER.COLUMN_REFRESH_S1';
                COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                        (
                                i_SqlStatement          => strSqlDynamic,
                                i_MaxTries              => 10,
                                i_WaitSeconds           => 10,
                                i_ShowStatement         => False,
                                i_ShowSuccess           => False,
                                i_RollbackOnRetry       => True,
                                o_Tries                 => intTries
                        );

                intRowCount     := to_number(COMMON_OWNER.SMT_CONTEXT.GET_ATTRIBUTE(i_AttributeName => 'RowCount'));

                strSqlCommand   := 'COMMIT';
                COMMIT;

                strMessage01    := '# of rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
                COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

                strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
                COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
                        (
                                i_TargetTableName       => 'COLUMN_REFRESH_S1',
                                i_Action                => 'INSERT',
                                i_RowCount              => intRowCount,
                                i_Comments              => 'Source: ALL_TAB_COLUMNS' || '@' || strSourceDbLink
                        );

                strColumnList   := '';

                strSqlCommand   := 'For recColumn';
                For recColumn in
                        (
                        SELECT
                                SRC.COLUMN_NAME,
                                SRC.COLUMN_ID
                          FROM  DLSTG_OWNER.COLUMN_REFRESH_S1   SRC,    -- Source columns
                                ALL_TAB_COLS                    TGT     -- Join to target columns to insure that the columns are in both source and target
                         WHERE  SRC.TABLE_NAME          = recTable.TABLE_NAME
                           AND  TGT.OWNER               = 'DLMRT_OWNER'
                           AND  TGT.TABLE_NAME          = SRC.TABLE_NAME
                           AND  TGT.COLUMN_NAME         = SRC.COLUMN_NAME
                           AND  TGT.VIRTUAL_COLUMN      = 'NO'
                        ORDER BY
                                SRC.COLUMN_ID
                        )
                Loop
                        strColumnList   := strColumnList || ',' || recColumn.COLUMN_NAME;
                End Loop; -- recColumn

                strColumnList   := trim(leading ',' from strColumnList);

                strMessage01    := 'Column list: ' || strColumnList;
                COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

                -- Build the insert for the target table
                If  strPartitioned = 'YES'
                Then
                        strHint         := '/*+APPEND*/';
                        strOrderBy      := ' ORDER BY INSTITUTION, CENSUS_PERIOD, CENSUS_SEQ';
                Else
                        strHint         := '';
                        strOrderBy      := '';
                End If;
                
                strSqlCommand   := 'Build refresh insert statement - ' || recTable.TABLE_NAME;
                strSqlDynamic   := '
                INSERT  ' || strHint || '
                  INTO  DLMRT_OWNER.' || recTable.TABLE_NAME || ' (' || strColumnList || ')
                SELECT  '|| strColumnList || '
                  FROM  DLMRT_OWNER.' || recTable.TABLE_NAME || '@' || strSourceDbLink || ' SRC
                 WHERE  SRC.INSTITUTION   = ''' || recCensus.INSTITUTION || '''
                   AND  SRC.CENSUS_PERIOD = ''' || recCensus.CENSUS_PERIOD || '''
                   AND  SRC.CENSUS_SEQ    = ' || recCensus.CENSUS_SEQ || '
                ' || strOrderBy
                ;

                strMessage01    := 'Inserting ' || recTable.TABLE_NAME || ' rows...';
                COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

                strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: INSERT INTO ' || recTable.TABLE_NAME;
                COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                        (
                                i_SqlStatement          => strSqlDynamic,
                                i_MaxTries              => 10,
                                i_WaitSeconds           => 10,
                                i_ShowStatement         => True,
                                i_ShowSuccess           => False,
                                i_RollbackOnRetry       => True,
                                o_Tries                 => intTries
                        );

                intRowCount     := to_number(COMMON_OWNER.SMT_CONTEXT.GET_ATTRIBUTE(i_AttributeName => 'RowCount'));

                strSqlCommand   := 'COMMIT';
                COMMIT;

                strMessage01    := '# of rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
                COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

                strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
                COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
                        (
                                i_TargetTableName       => recTable.TABLE_NAME,
                                i_Action                => 'INSERT',
                                i_RowCount              => intRowCount,
                                i_Comments              => 'Source: ' || recTable.TABLE_NAME || '@' || strSourceDbLink
                        );

                strSqlCommand   := 'SMT_INDEX.ALL_REBUILD';
                COMMON_OWNER.SMT_INDEX.ALL_REBUILD
                                (
                                i_TableOwner            => 'DLMRT_OWNER',
                                i_TableName             => recTable.TABLE_NAME,
                                i_UnusableOnly          => True,
                                i_AllJoinedTables       => False,
                                i_ParallelDegree        => 1,
                                i_DetailLogging         => False
                                );

                strMessage01    := 'Gathering ' || recTable.TABLE_NAME || ' statistics...';
                COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

                strSqlCommand   := 'DBMS_STATS.GATHER_TABLE_STATS';
                DBMS_STATS.GATHER_TABLE_STATS
                        (
                                OWNNAME         => 'DLMRT_OWNER',
                                TABNAME         => recTable.TABLE_NAME,
                                GRANULARITY     => 'AUTO',
                                DEGREE          => 16
                        );

                strMessage01    := 'Statistics gathered...';
                COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

                intTableRefreshCount    := intTableRefreshCount + 1;
        End Loop; -- recTable
        intCensusCount  := intCensusCount + 1;
End Loop; -- recCensus


strClientInfo   := i_ProcessName;
strSqlCommand   := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strClientInfo);

strMessage01    := 'Number of censuses copied: ' || to_char(intCensusCount);
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strMessage01    := 'Number of tables copied: ' || to_char(intTableRefreshCount);
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := i_ProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

EXCEPTION
    WHEN OTHERS THEN

        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );

END "CensusCopy";
/
