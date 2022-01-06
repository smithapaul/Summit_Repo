CREATE OR REPLACE PROCEDURE             "UM_M_AF_STDNT_ADM_AMH_DAILY_P"  
           (
                   i_EFFDT      in  Varchar2    Default SYSDATE
           )

   IS
------------------------------------------------------------------------
-- Kieu Tran
--
-- Loads table UM_M_AF_STDNT_ADM_AMH_DAILY.
--
 --V01  Case-xxxxx 0/09/2021,    Kieu Tran
--                               New Process
--
------------------------------------------------------------------------

--  Format for parameter i_EFFDT is DD-MON-YYYY i.e. 25-DEC-2021

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_M_AF_STDNT_ADM_AMH_DAILY_P';
        strTargetTableOwner             Varchar2(128)   := 'CSMRT_OWNER';
        strTargetTableName              Varchar2(128)   := 'UM_M_AF_STDNT_ADM_AMH_DAILY';
        strTarget                       Varchar2(100)   := 'CSMRT_OWNER.UM_M_AF_STDNT_ADM_AMH_DAILY';
        dtEFFDT                         Date            := to_date(i_EFFDT);        
        strPartitionName                Varchar2(128)   := 'DATE_' || to_char(dtEFFDT,'YYYY_MM_DD');
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(32767);
        intTries                        Integer;
        strPartitionExists              Varchar2(1);
        intProcessSid                   Integer;
		strInstance                     VARCHAR2(100);
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTablePartitionRenameCount    Integer;
        intIndexPartitionRenameCount    Integer;
        ProcessedStatus                 Varchar2(50);
        M_LoadStatus                    Varchar2(50);        

BEGIN

   --verifying the job is not running twice on the same day and checking whether UM_S_AF_STDNT_ADM_AMH table loaded with the current day's file 
   
    select decode(count(*),0,'NOTPROCESSED','PROCESSED') into ProcessedStatus 
      from (
              select  max(RUN_DT) RUN_DT
              from CSMRT_OWNER.UM_M_AF_STDNT_ADM_AMH_DAILY
           )
    where (RUN_DT = trunc(dtEFFDT));

    select decode(count(*),0,'SOURCEEMPTY','SOURCELOADED') into M_LoadStatus 
    from CSMRT_OWNER.UM_M_AF_STDNT_ADM_AMH  
    where trunc(created_ew_dttm) = trunc(sysdate);
    

    
    strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
    DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

    strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
    COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
            (
                    i_MartId                => strMartId,
                    i_ProcessName           => strProcessName,
                    i_ProcessStartTime      => dtProcessStart
        );
        
If  ProcessedStatus = 'NOTPROCESSED' AND M_LoadStatus = 'SOURCELOADED'   THEN

    strMessage01    := 'Truncating daily partition for ' || strTarget;
    COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

    strSqlCommand := 'SELECT strPartitionExists';
    SELECT  CASE
              WHEN  PARTITION_NAME IS NULL
              THEN  'N'
              ELSE  'Y'
            END     PARTITION_EXISTS
      INTO  strPartitionExists
      FROM  (
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

    strSqlCommand   := 'Truncate partition if it exists';
    If  strPartitionExists = 'Y'
    Then

    strMessage01    := 'Truncating partition ' || strPartitionName || ' for table ' || strTarget;
    COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

    strSqlDynamic   := 'ALTER TABLE ' || strTarget || ' TRUNCATE PARTITION "' || strPartitionName || '" UPDATE INDEXES';
    strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
    COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                    (
                    i_SqlStatement                  => strSqlDynamic,
                    i_MaxTries                      => 10,
                    i_WaitSeconds                   => 10,
                    o_Tries                         => intTries
                    );

    End If;

    strMessage01    := 'Inserting data into ' || strTarget;
    COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

    insert /*+ append */ into CSMRT_OWNER.UM_M_AF_STDNT_ADM_AMH_DAILY
    select trunc(dtEFFDT) RUN_DT,
           INSTITUTION_CD, ACAD_CAR_CD, ACAD_PROG_CD, ACAD_PLAN_CD, ADMIT_TERM_CD, PERSON_ID, ADM_APPL_NBR, SLATE_ID, EXT_ADM_APPL_NBR, SRC_SYS_ID, INSTITUTION_LD, ACAD_CAR_LD, 
           ACAD_PROG_LD, ACAD_PLAN_LD, ADMIT_TERM_LD, REPORTING_TERM_CD, REPORTING_TERM_LD, ACAD_YR, FISCAL_YR, ACAD_ORG_CD, ACAD_ORG_LD, ADMIT_TYPE_ID, ADMIT_TYPE_LD, ADMIT_TYPE_GRP, 
           APPL_CNTR_ID, CE_APPL_FLG, EDU_LVL_CD, EDU_LVL_LD, IS_RSDNCY_FLG, PLAN_CIP_CD, PLAN_CIP_LD, RSDNCY_ID, RSDNCY_LD, APPL_CNT, ADMIT_CNT, DENY_CNT, DEPOSIT_CNT, ENROLL_CNT, 
           ENROLL_SUBSEQ_CNT, UNDUP_CNT, SYSDATE CREATED_EW_DTTM
      from CSMRT_OWNER.UM_M_AF_STDNT_ADM_AMH
    ;

    strSqlCommand   := 'SET intRowCount';
    intRowCount     := SQL%ROWCOUNT;

    strSqlCommand := 'commit';
    commit;

    strMessage01    := '# of rows inserted into ' || strTarget || ': ' || TO_CHAR(intRowCount,'999,999,999,999');
    COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

    strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
    COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
            (
                    i_TargetTableName   => strTarget,
                    i_Action            => 'INSERT',
                    i_RowCount          => intRowCount
            );

                    strSqlCommand   := 'SMTCMN_PART.PARTITION_RENAME';
                    COMMON_OWNER.SMTCMN_PART.PARTITION_RENAME
                            (
                                    i_TableOwner                    => strTargetTableOwner,
                                    i_TableName                     => strTargetTableName,
                                    i_PartitionKey                  => 'DATE',
                                    o_TablePartitionRenameCount     => intTablePartitionRenameCount,
                                    o_IndexPartitionRenameCount     => intIndexPartitionRenameCount
                            );

                    COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => '# of table partitions renamed: ' || TO_CHAR(intTablePartitionRenameCount));
                    COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => '# of index partitions renamed: ' || TO_CHAR(intIndexPartitionRenameCount));

    strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
    COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

    strMessage01    := strProcessName || ' is complete.';
    COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

ELSE

   strMessage01    := 'UM_M_AF_STDNT_ADM_AMH_DAILY process is not eligible to proceed. Processed Status: ' || ProcessedStatus || ' and M Load Status: '|| M_LoadStatus ;
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

End If;

EXCEPTION
    WHEN OTHERS THEN
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );

END UM_M_AF_STDNT_ADM_AMH_DAILY_P;
/
