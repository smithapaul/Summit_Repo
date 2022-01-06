CREATE OR REPLACE PROCEDURE             "UMOL_Truncate"
        (
                i_Owner         in  Varchar2,
                i_Table         in  Varchar2,
                i_Partition     in  Varchar2,
                i_ContextBlock  in  Varchar2    Default Null
        )
 AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--
-- V01  xxxx 03/02/2020     George Adams
--                              Truncate partition in CSSTG_OWNER.UMOL_xxx table.
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'UMOL';        -- Get Mart_ID as argument???   -- Default to SMT for Summit???
        strProcessName                  Varchar2(100)   := 'UMOL_Truncate';
        dtProcessStart                  Date            := SYSDATE;
        strTargetTableOwner             Varchar2(128)   := i_Owner;
        strTargetTableName              Varchar2(128)   := i_Table;
        strPartitionName                Varchar2(128)   := i_Partition;
        strTargetFullTableName          Varchar2(128)   := i_Owner || '.' || i_Table;
        strMessage01                    Varchar2(32767);
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strLineFeed                     Varchar2(2)     := chr(10);
        strCarriageReturn               Varchar2(2)     := chr(13);
        strSqlCommand                   Varchar2(32767) := '';
        strSqlDynamic                   Varchar2(32767) := '';
        intRowCountBefore               Integer;
        intRowCountAfter                Integer;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(32767);
        intTries                        Integer;
        strTableExists                  Varchar2(1);
        strPartitionExists              Varchar2(1);
        strTimeMask                     Varchar2(22)    := 'DD-MON-YYYY HH24:MI:SS';
        strProcessStartTime             Varchar2(20)    := to_char(dtProcessStart,strTimeMask);

BEGIN

strSqlCommand   := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

If  i_ContextBlock is null
Then
        strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_INIT';
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
                (
                        i_MartId                => strMartId,
                        i_ProcessName           => strProcessName,
                        i_ProcessStartTime      => dtProcessStart
                );
Else
        strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_CONTEXT';
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_CONTEXT
                (
                        i_ContextBlock          => i_ContextBlock
                );
End If;

strMessage01    := 'Procedure CSMRT_OWNER.' || strProcessName || ' arguments:'
                || strNewLine || ' i_Owner: '     || i_Owner
                || strNewLine || ' i_Table: '     || i_Table
                || strNewLine || ' i_Partition: '     || i_Partition
                || strNewLine || ' i_ContextBlock: ' || i_ContextBlock;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

-- Validate table and partition

strSqlCommand := 'SELECT strTableExists, strPartitionExists';
SELECT  CASE
          WHEN  TABLE_NAME IS NULL
          THEN  'N'
          ELSE  'Y'
        END     TABLE_EXISTS,
        CASE
          WHEN  PARTITION_NAME IS NULL
          THEN  'N'
          ELSE  'Y'
        END     PARTITION_EXISTS
  INTO  strTableExists,
        strPartitionExists
  FROM  (
        SELECT  (
                SELECT  TAB.TABLE_NAME
                  FROM  ALL_TABLES TAB
                 WHERE  TAB.OWNER               = strTargetTableOwner
                   AND  TAB.TABLE_NAME          = strTargetTableName
--                   AND  TAB.TABLE_NAME          like 'UMOL%'
                )                                       TABLE_NAME
        FROM  DUAL
        ),
        (
        SELECT  (
                SELECT  PRT.PARTITION_NAME
                  FROM  ALL_TAB_PARTITIONS PRT
                 WHERE  PRT.TABLE_OWNER         = strTargetTableOwner
                   AND  PRT.TABLE_NAME          = strTargetTableName
                   AND  PRT.PARTITION_NAME      = strPartitionName
                )                                       PARTITION_NAME
        FROM  DUAL
        )
;

strSqlCommand   := 'Validate table existence';
If  strTableExists = 'N'
Then
        RAISE_APPLICATION_ERROR( -20001, 'Table ' || strTargetFullTableName || ' does not exist.');
End If;

strSqlCommand   := 'Validate partition existence';
If  strPartitionExists = 'N'
Then
        RAISE_APPLICATION_ERROR( -20001, 'Parition ' || strPartitionName || ' does not exist in table ' || strTargetFullTableName);
End If;

strMessage01    := 'Truncating partition ' || strPartitionName || ' of table ' || strTargetFullTableName;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'ALTER TABLE ' || strTargetFullTableName || ' TRUNCATE PARTITION ' || strPartitionName || ' UPDATE INDEXES';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement                  => strSqlDynamic,
                i_MaxTries                      => 10,
                i_WaitSeconds                   => 10,
                o_Tries                         => intTries
                );

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName       => strTargetFullTableName,
                i_Action                => 'TRUNCATE',
                i_RowCount              => intRowCountBefore,
                i_Comments              => 'Partition: ' || strPartitionName
        );

If  i_ContextBlock is null
Then
        strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;
End If;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

EXCEPTION
    WHEN OTHERS THEN
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );

END "UMOL_Truncate";
/
