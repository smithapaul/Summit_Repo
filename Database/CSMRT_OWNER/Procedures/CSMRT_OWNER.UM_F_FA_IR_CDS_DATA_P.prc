CREATE OR REPLACE PROCEDURE             "UM_F_FA_IR_CDS_DATA_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads mart table UM_F_FA_IR_CDS_DATA.
--
 --V01  SMT-xxxx 07/12/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_IR_CDS_DATA';
        intProcessSid                   Integer;
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
        intTries                        Integer;

BEGIN
strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => strMartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_IR_CDS_DATA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_IR_CDS_DATA');

--alter table UM_F_FA_IR_CDS_DATA disable constraint PK_UM_F_FA_IR_CDS_DATA;
strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_IR_CDS_DATA disable constraint PK_UM_F_FA_IR_CDS_DATA';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_IR_CDS_DATA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_IR_CDS_DATA';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_IR_CDS_DATA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_IR_CDS_DATA';				
insert /*+ append */ into CSMRT_OWNER.UM_F_FA_IR_CDS_DATA
SELECT /*+ parallel(8) inline */
  STG.INSTITUTION INSTITUTION_CD,
  STG.CAREER ACAD_CAR_CD,
  STG.TERM TERM_CD,
  STG.EMPLID PERSON_ID, 
  NVL(INST.INSTITUTION_SID, 2147483646) INSTITUTION_SID,
  NVL(CAR.ACAD_CAR_SID, 2147483646) ACAD_CAR_SID,
  NVL(TERM.TERM_SID, 2147483646) TERM_SID,
  NVL(PER.PERSON_SID, 2147483646) PERSON_SID, 
  NVL(RSDN.RSDNCY_SID, 2147483646) RSDNCY_SID,  
  STG.RESIDENCY_CD RESIDENCY_CD,
  NVL(LOAD.ACAD_LOAD_SID, 2147483646) ACAD_LOAD_SID, 
  STG.ACAD_LOAD_CD ACAD_LOAD_CD,
  STG.FIRST_FULL_FRESHMAN_FLAG FIRST_FULL_FRESHMAN_FLAG,
  SYSDATE CREATED_EW_DTTM,
  SYSDATE LASTUPD_EW_DTTM
FROM 
  CSSTG_OWNER.UM_FA_IR_CDS_STG STG
    LEFT OUTER JOIN PS_D_ACAD_CAR CAR ON STG.CAREER = CAR.ACAD_CAR_CD
                   AND STG.INSTITUTION = CAR.INSTITUTION_CD
    LEFT OUTER JOIN PS_D_ACAD_LOAD LOAD ON STG.ACAD_LOAD_CD = LOAD.ACAD_LOAD_CD
                   AND LOAD.APPRVD_IND IN ('-', 'Y')
    LEFT OUTER JOIN PS_D_INSTITUTION INST ON STG.INSTITUTION = INST.INSTITUTION_CD
    LEFT OUTER JOIN PS_D_PERSON PER ON STG.EMPLID = PER.PERSON_ID
    LEFT OUTER JOIN PS_D_RSDNCY RSDN ON STG.RESIDENCY_CD = RSDN.RSDNCY_ID
    LEFT OUTER JOIN PS_D_TERM TERM ON STG.TERM = TERM.TERM_CD
                   AND STG.INSTITUTION = TERM.INSTITUTION_CD
                   AND STG.CAREER = TERM.ACAD_CAR_CD
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_IR_CDS_DATA rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_IR_CDS_DATA',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );



strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_IR_CDS_DATA',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_IR_CDS_DATA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
--alter table UM_F_FA_IR_CDS_DATA enable constraint PK_UM_F_FA_IR_CDS_DATA;

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_IR_CDS_DATA enable constraint PK_UM_F_FA_IR_CDS_DATA';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_IR_CDS_DATA');

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

END UM_F_FA_IR_CDS_DATA_P;
/
