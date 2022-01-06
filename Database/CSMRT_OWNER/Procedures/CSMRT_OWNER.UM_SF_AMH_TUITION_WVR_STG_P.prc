CREATE OR REPLACE PROCEDURE             UM_SF_AMH_TUITION_WVR_STG_P AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- James Doucette
--
-- Loads stage table CSSTG_OWNER.UM_SF_AMH_TUITION_WVR_STG.
--
-- V01  SMT-xxxx 02/12/2020,    James Doucette
--                              Converted from VendorLocHighMatchS2.sql
--V02   SMT-xxxx 07/14/2021,    Srikanth automated add partition , truncate partition based on 
--                              UM_SF_AMH_TUITION_WVR_TMP table
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_SF_AMH_TUITION_WVR_STG_P';
        dtProcessStart                  Date            := SYSDATE;
        intProcessSid                   INTEGER;
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
        strPartitionExists              Varchar2(1);
        intPartitionIndicatorCurrent    INTEGER;
        intPartitionIndicatorNew        INTEGER;
        strPartitionNameNew             VARCHAR2(30);
        strCarriageReturn               Varchar2(1) := chr(13);
        rtpTarget                       CSSTG_OWNER.UM_SF_AMH_TUITION_WVR_STG%ROWTYPE; -- Creates a record with columns matching those in the target table
        intErrorCount                   Integer := 0;
        intRowNum                       Integer;
        intHeaderRowCount               Integer := 0;
        bolError                        Boolean;
        intInsertCount                  Integer := 0;
        intFailedRowCount               Integer := 0;
        intFailedRowMax                 Integer := 10;
 
  CURSOR t_cur                           ---This CURSOR provides the distinct partition_key from temp table
      IS     
    SELECT 
    DISTINCT PARTITION_KEY
    FROM CSSTG_OWNER.UM_SF_AMH_TUITION_WVR_TMP; 
    
    
     t_rec                           t_cur%ROWTYPE;
     V_REF_CUR_QUERY                 Varchar2(32767) :='';    

BEGIN

strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => strMartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );

strSqlCommand := 'SMT_INTERFACE.INTERFACE_INIT';
COMMON_OWNER.SMT_INTERFACE.INTERFACE_INIT
                (       i_SourceTableOwner      => 'CSSTG_OWNER',
                        i_SourceTableName       => 'UM_SF_AMH_TUITION_WVR_TMP',
                        i_TargetTableOwner      => 'CSSTG_OWNER',
                        i_TargetTableName       => 'UM_SF_AMH_TUITION_WVR_STG'
                );
 
 OPEN t_cur;                                                    ----CURSOR open and loop through to check partition exists on target table 
 LOOP
 FETCH t_cur INTO t_rec;
 EXIT WHEN t_cur%NOTFOUND;
 DBMS_OUTPUT.PUT_LINE ('row count ='|| t_cur%ROWCOUNT || ' ' ||t_rec.PARTITION_KEY);
 
 SELECT CASE
          WHEN  PARTITION_NAME IS NULL
          THEN  'N'
          ELSE  'Y'
        END     PARTITION_EXISTS
  INTO  
     strPartitionExists
  FROM  
        (
        SELECT  (
                SELECT  PRT.PARTITION_NAME
                  FROM  ALL_TAB_PARTITIONS PRT
                 WHERE  PRT.TABLE_OWNER         = 'CSSTG_OWNER'
                   AND  PRT.TABLE_NAME          = 'UM_SF_AMH_TUITION_WVR_STG'
                   AND  PRT.PARTITION_NAME      = t_rec.PARTITION_KEY
                )                                       PARTITION_NAME
        FROM  DUAL
        )
;
DBMS_OUTPUT.PUT_LINE ('row count = '|| t_cur%ROWCOUNT || '  ' ||strPartitionExists);
    IF strPartitionExists = 'Y'                    ---validate if table already has partition then truncate the partition
    THEN
        strSqlDynamic :=
               'ALTER TABLE CSSTG_OWNER.UM_SF_AMH_TUITION_WVR_STG TRUNCATE PARTITION '
                 || t_rec.PARTITION_KEY
                 || ' UPDATE INDEXES';
        strSqlCommand := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
        COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE (
            i_SqlStatement   => strSqlDynamic,
            i_MaxTries       => 10,
            i_WaitSeconds    => 10,
            o_Tries          => intTries);
    END IF;
   
    
    IF strPartitionExists = 'N'                     ---validate if table already has partition else add the new partition
    THEN
        V_REF_CUR_QUERY :=
               'ALTER TABLE CSSTG_OWNER.UM_SF_AMH_TUITION_WVR_STG ADD PARTITION '
            || t_rec.PARTITION_KEY
            || ' values'
            || '('
            ||''''
            || t_rec.PARTITION_KEY
            ||''''
            || ')';

      EXECUTE IMMEDIATE V_REF_CUR_QUERY;
      DBMS_OUTPUT.PUT_LINE (V_REF_CUR_QUERY);
      --DBMS_OUTPUT.PUT_LINE ('RAN SUCCESSFULLY');
     END IF;
END LOOP;


strMessage01    := 'Disabling Indexes for table CSSTG_OWNER.UM_SF_AMH_TUITION_WVR_STG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_SF_AMH_TUITION_WVR_STG');

--alter table CSSTG_OWNER.UM_SF_AMH_TUITION_WVR_STG disable constraint PK_UM_SF_AMH_TUITION_WVR_STG; 
strSqlDynamic   := 'alter table CSSTG_OWNER.UM_SF_AMH_TUITION_WVR_STG disable constraint PK_UM_SF_AMH_TUITION_WVR_STG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic; 

strMessage01    := 'Loading data into CSSTG_OWNER.UM_SF_AMH_TUITION_WVR_STG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'Insert into CSSTG_OWNER.UM_SF_AMH_TUITION_WVR_STG';
insert /*+ append */  into CSSTG_OWNER.UM_SF_AMH_TUITION_WVR_STG                     ---insert into stage table
select /*+ full(S) */
    INSTITUTION_CD, 
    ITEM_TERM, 
    ITEM_TERM_LD, 
    PERSON_ID, 
    FIRST_NAME, 
    LAST_NAME, 
    ITEM_TYPE, 
    ITEM_TYPE_LD, 
    ACAD_CAREER, 
    ACAD_YEAR, 
    SPONSOR_ID, 
    EMPLOYEE_TYPE, 
    ITEM_AMT, 
    AMOUNT_TYPE, 
    DAY_CE_FLAG, 
    TAXABLE_FLAG, 
    STUDENT_IS_EMPLOYEE_FLAG, 
    PARTITION_KEY, 
    TO_DATE(CREATED_EW_DTTM, 'MM/DD/YYYY') AS CREATED_EW_DTTM 
FROM CSSTG_OWNER.UM_SF_AMH_TUITION_WVR_TMP
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_SF_AMH_TUITION_WVR_STG rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_SF_AMH_TUITION_WVR_STG',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Indexing CSSTG_OWNER.UM_SF_AMH_TUITION_WVR_STG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Indexing',
       END_DT = NULL
 where TABLE_NAME = 'UM_SF_AMH_TUITION_WVR_STG'
;

strSqlCommand := 'commit';
commit;



strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = sysdate,
       OLD_MAX_SCN = 0,
       NEW_MAX_SCN = 999999999999
 where TABLE_NAME = 'UM_SF_AMH_TUITION_WVR_STG'
;

strSqlCommand := 'commit';
commit;
-- JRD INSERT END


-- JRD ENABLE INDEXES BEGIN
strMessage01    := 'Enabling Indexes for table CSSTG_OWNER.UM_SF_AMH_TUITION_WVR_STG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_SF_AMH_TUITION_WVR_STG');

--alter table CSSTG_OWNER.UM_SF_AMH_TUITION_WVR_STG enable constraint PK_UM_SF_AMH_TUITION_WVR_STG; 
strSqlDynamic   := 'alter table CSSTG_OWNER.UM_SF_AMH_TUITION_WVR_STG enable constraint PK_UM_SF_AMH_TUITION_WVR_STG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );



strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

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

END UM_SF_AMH_TUITION_WVR_STG_P;
/
