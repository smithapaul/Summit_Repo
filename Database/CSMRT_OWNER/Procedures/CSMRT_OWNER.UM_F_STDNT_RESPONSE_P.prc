CREATE OR REPLACE PROCEDURE             "UM_F_STDNT_RESPONSE_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table UM_F_STDNT_RESPONSE from PeopleSoft table UM_F_STDNT_RESPONSE.
--
 --V01  SMT-xxxx 06/18/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_STDNT_RESPONSE';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_STDNT_RESPONSE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_STDNT_RESPONSE', TRUE);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_RESPONSE disable constraint PK_UM_F_STDNT_RESPONSE';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_STDNT_RESPONSE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_STDNT_RESPONSE';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_STDNT_RESPONSE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_STDNT_RESPONSE';				
insert into UM_F_STDNT_RESPONSE
with RSN as (
select INSTITUTION, ACAD_CAREER, RESPONSE_REASON, SRC_SYS_ID, 
       DESCR, DESCRSHORT,
       row_number() over (partition by INSTITUTION, ACAD_CAREER, RESPONSE_REASON
                              order by EFFDT desc) RSN_ORDER
  from CSSTG_OWNER.PS_RESP_RSN_TBL
 where DATA_ORIGIN <> 'D'
)
select ADM.EMPLID PERSON_ID, 
       ADM.ACAD_CAREER ACAD_CAR_CD, 
       ADM.STDNT_CAR_NBR, 
       ADM.ADM_APPL_NBR, 
       nvl(RESP.SEQNUM,0) SEQNUM, 
       ADM.SRC_SYS_ID,
       ADM.INSTITUTION INSTITUTION_CD,
       nvl(I.INSTITUTION_SID,2147483646) INSTITUTION_SID,  
       nvl(P.PERSON_SID,2147483646) PERSON_SID, 
       nvl(C.ACAD_CAR_SID,2147483646) ACAD_CAR_SID,
       nvl(E.EXT_ORG_SID,2147483646) EXT_ORG_SID,
       RESP.RESPONSE_REASON, 
       RESP.RESPONSE_DT, 
       RESP.EXT_ORG_ID, 
       RESP.DESCR,
       RSN.DESCR RSN_DESCR, 
       RSN.DESCRSHORT RSN_DESCRSHORT,
       'N','S',SYSDATE,SYSDATE,1234
  from CSSTG_OWNER.PS_ADM_APPL_DATA ADM
  join CSSTG_OWNER.PS_STDNT_RESPONSE RESP
    on ADM.EMPLID = RESP.EMPLID 
   and ADM.ACAD_CAREER = RESP.ACAD_CAREER
   and ADM.STDNT_CAR_NBR = RESP.STDNT_CAR_NBR 
   and ADM.ADM_APPL_NBR = RESP.ADM_APPL_NBR
   and ADM.SRC_SYS_ID = RESP.SRC_SYS_ID
   and RESP.DATA_ORIGIN <> 'D'
  left outer join RSN
    on ADM.INSTITUTION = RSN.INSTITUTION 
   and RESP.ACAD_CAREER = RSN.ACAD_CAREER
   and RESP.RESPONSE_REASON = RSN.RESPONSE_REASON
   and RESP.SRC_SYS_ID = RSN.SRC_SYS_ID
   and RSN.RSN_ORDER = 1 
  left outer join PS_D_INSTITUTION I
    on ADM.INSTITUTION = I.INSTITUTION_CD
   and ADM.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  left outer join PS_D_ACAD_CAR C
    on ADM.INSTITUTION = C.INSTITUTION_CD
   and ADM.ACAD_CAREER = C.ACAD_CAR_CD
   and ADM.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  left outer join PS_D_PERSON P
    on ADM.EMPLID = P.PERSON_ID 
   and ADM.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'
  left outer join PS_D_EXT_ORG E    
    on RESP.EXT_ORG_ID = E.EXT_ORG_ID
   and ADM.SRC_SYS_ID = E.SRC_SYS_ID   
   and E.DATA_ORIGIN <> 'D'
 where ADM.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_STDNT_RESPONSE rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_RESPONSE',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_RESPONSE',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_STDNT_RESPONSE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_RESPONSE enable constraint PK_UM_F_STDNT_RESPONSE';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_STDNT_RESPONSE');

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

END UM_F_STDNT_RESPONSE_P;
/
