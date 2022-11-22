DROP PROCEDURE CSMRT_OWNER."RoleDefnStage"
/

--
-- "RoleDefnStage"  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."RoleDefnStage" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Greg Kampf 08/25/2016
--
-- DESCRIPTION
--
-- V01  SMT-6452 08/25/2016,    Greg Kampf
--                              Load stage table PSROLEDEFN from PS.
-- V02           03/01/2017,    George Adams
--                              Modified Exception handling.
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'RoleDefnStage';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    VARCHAR2(4000);
        strMessage02                    VARCHAR2(512);
        strMessage03                    VARCHAR2(512)   :='';
        strNewLine                      VARCHAR2(2)     := chr(13) || chr(10);
        strSqlCommand                   VARCHAR2(32767) :='';
        strSqlDynamic                   VARCHAR2(32767) :='';
        strClientInfo                   VARCHAR2(100);
        intRowCount                     INTEGER;
        intTotalRowCount                INTEGER         := 0;
        numSqlCode                      NUMBER;
        strSqlErrm                      VARCHAR2(4000);
        intTries                        INTEGER;
        intYear                         INTEGER;
        strControlRowExists             VARCHAR2(1);
        intPartitionIndicatorCurrent    INTEGER;
        intPartitionIndicatorNew        INTEGER;
        strPartitionNameNew             VARCHAR2(30);

BEGIN
strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => strMartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart
        );

strSqlCommand := 'SELECT PARTITION_INDICATOR FROM PARTITION_SWAP_CONTROL';
SELECT  CASE
          WHEN  PARTITION_INDICATOR IS NULL
          THEN  'N'
          ELSE  'Y'
        END     CONTROL_ROW_EXISTS,
        PARTITION_INDICATOR,
        CASE
          WHEN  PARTITION_INDICATOR IS NULL
          THEN  0
          WHEN  PARTITION_INDICATOR = 0
          THEN  1
          ELSE  0
        END     PARTITION_INDICATOR_NEW
  INTO  strControlRowExists,
        intPartitionIndicatorCurrent,
        intPartitionIndicatorNew
  FROM  (
        SELECT  (
                SELECT  PSC.PARTITION_INDICATOR
                  FROM  CSSTG_OWNER.PARTITION_SWAP_CONTROL PSC
                 WHERE  PSC.TABLE_OWNER = 'CSSTG_OWNER'
                   AND  PSC.TABLE_NAME  = 'PSROLEDEFN'
                )                                       PARTITION_INDICATOR
        FROM  DUAL
        )
;

strPartitionNameNew     := 'PARTITION_' || TO_CHAR(intPartitionIndicatorNew);

strMessage01            := 'Loading partition ' || strPartitionNameNew || ' of table PSROLEDEFN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'ALTER TABLE CSSTG_OWNER.PSROLEDEFN TRUNCATE PARTITION ' || strPartitionNameNew || ' UPDATE INDEXES';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement                  => strSqlDynamic,
                i_MaxTries                      => 10,
                i_WaitSeconds                   => 10,
                o_Tries                         => intTries
                );

strSqlCommand   := 'SMT_INDEX.ALL_UNUSABLE (PSROLEDEFN)';
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE
                (
                i_TableOwner                    => 'CSSTG_OWNER',
                i_TableName                     => 'PSROLEDEFN',
                i_IncludeJoinedTables           => False,
                i_IncludePartitionedIndexes     => True,
                i_PartitionName                 => strPartitionNameNew,
                i_BitmapsOnly                   => True,
                i_IndexNameNotLike              => 'PK%'
                );

strMessage01            := 'Inserting PSROLEDEFN rows...';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'INSERT INTO CSSTG_OWNER.PSROLEDEFN';
INSERT  /*+APPEND*/
  INTO  CSSTG_OWNER.PSROLEDEFN
        (
        ROLENAME,
        VERSION,
        ROLETYPE,
        DESCR,
        QRYNAME,
        ROLESTATUS,
        RECNAME,
        FIELDNAME,
        PC_EVENT_TYPE,
        QRYNAME_SEC,
        PC_FUNCTION_NAME,
        ROLE_PCODE_RULE_ON,
        ROLE_QUERY_RULE_ON,
        LDAP_RULE_ON,
        ALLOWNOTIFY,
        ALLOWLOOKUP,
        LASTUPDDTTM,
        LASTUPDOPRID,
        DESCRLONG,
        PARTITION_INDICATOR,
        LOADTIME
        )
SELECT
        SRC.ROLENAME,
        SRC.VERSION,
        SRC.ROLETYPE,
        SRC.DESCR,
        SRC.QRYNAME,
        SRC.ROLESTATUS,
        SRC.RECNAME,
        SRC.FIELDNAME,
        SRC.PC_EVENT_TYPE,
        SRC.QRYNAME_SEC,
        SRC.PC_FUNCTION_NAME,
        SRC.ROLE_PCODE_RULE_ON,
        SRC.ROLE_QUERY_RULE_ON,
        SRC.LDAP_RULE_ON,
        SRC.ALLOWNOTIFY,
        SRC.ALLOWLOOKUP,
        SRC.LASTUPDDTTM,
        SRC.LASTUPDOPRID,
        SRC.DESCRLONG,
        intPartitionIndicatorNew        PARTITION_INDICATOR,
        SYSDATE                         LOADTIME
  FROM  SYSADM.PSROLEDEFN@SASOURCE          SRC
;

intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'COMMIT (PSROLEDEFN INSERTS)';
COMMIT;

strMessage01 := '# of rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
                (
                i_TargetTableName       => 'PSROLEDEFN',
                i_Action                => 'INSERT',
                i_RowCount              => intRowCount
                );

strSqlCommand   := 'SMT_INDEX.ALL_REBUILD';
COMMON_OWNER.SMT_INDEX.ALL_REBUILD
                (
                i_TableOwner            => 'CSSTG_OWNER',
                i_TableName             => 'PSROLEDEFN',
                i_UnusableOnly          => True,
                i_AllJoinedTables       => False,
                i_ParallelDegree        => 1
                );

If  strControlRowExists = 'Y'
Then
        strSqlCommand   := 'UPDATE CSSTG_OWNER.PARTITION_SWAP_CONTROL';
        UPDATE  CSSTG_OWNER.PARTITION_SWAP_CONTROL
           SET  PARTITION_INDICATOR     = intPartitionIndicatorNew,
                LOADTIME                = SYSDATE,
                UPDATED_BY              = strProcessName
         WHERE  TABLE_OWNER     = 'CSSTG_OWNER'
           AND  TABLE_NAME      = 'PSROLEDEFN'
        ;
Else
        strSqlCommand   := 'INSERT INTO CSSTG_OWNER.PARTITION_SWAP_CONTROL';
        INSERT
          INTO  CSSTG_OWNER.PARTITION_SWAP_CONTROL
                (
                TABLE_OWNER,
                TABLE_NAME,
                PARTITION_INDICATOR,
                LOADTIME,
                UPDATED_BY
                )
        VALUES  (
                'CSSTG_OWNER',
                'PSROLEDEFN',
                intPartitionIndicatorNew,
                SYSDATE,
                strProcessName
                )
        ;
End If;

strSqlCommand := 'COMMIT (PARTITION_SWAP_CONTROL DML)';
COMMIT;

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01            := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

EXCEPTION
    WHEN OTHERS THEN
        numSqlCode := SQLCODE;
        strSqlErrm := SQLERRM;

        ROLLBACK;
  
        strMessage01 := 'Error code: ' || TO_CHAR(SQLCODE) || ' Error Message: ' || SQLERRM;
        strMessage02 := TO_CHAR(SQLCODE);
  
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_FAILURE
                       (i_SqlCommand    => strSqlCommand,
                        i_ErrorText     => strMessage01,
                        i_ErrorCode     => strMessage02,
                        i_ErrorMessage  => strSqlErrm
                       );
               
        strMessage01 := 'Error...'
                        || strNewLine   || 'SQL Command:   ' || strSqlCommand
                        || strNewLine   || 'Error code:    ' || numSqlCode
                        || strNewLine   || strSqlErrm
                        || strNewLine   || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;

        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
        RAISE_APPLICATION_ERROR( -20001, strMessage01);

END "RoleDefnStage";
/
