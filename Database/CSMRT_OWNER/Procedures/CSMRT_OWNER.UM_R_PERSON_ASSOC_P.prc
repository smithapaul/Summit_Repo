CREATE OR REPLACE PROCEDURE             "UM_R_PERSON_ASSOC_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_R_PERSON_ASSOC
--V01 12/12/2018             -- srikanth ,pabbu converted to proc from sql scripts
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_R_PERSON_ASSOC';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_R_PERSON_ASSOC';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_R_PERSON_ASSOC';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_R_PERSON_ASSOC';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_R_PERSON_ASSOC');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_R_PERSON_ASSOC disable constraint PK_UM_R_PERSON_ASSOC';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_R_PERSON_ASSOC';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_R_PERSON_ASSOC';
insert /*+ append enable_parallel_dml parallel(8) */ into CSMRT_OWNER.UM_R_PERSON_ASSOC
  with
       Q0 as (
select /*+ inline parallel(8) no_merge */
       I.INSTITUTION_CD, P.PERSON_ID, P.SRC_SYS_ID, P.PERSON_SID
  from CSMRT_OWNER.PS_D_INSTITUTION I
  join CSMRT_OWNER.PS_D_PERSON P
    on I.INSTITUTION_SID < 2147483646),
       Q1 as (
select /*+ inline parallel(8)  */
       EMPLID PERSON_ID, INSTITUTION INSTITUTION_CD, STDNT_GROUP STDNT_GRP_CD, EFFDT, SRC_SYS_ID,
       EFF_STATUS EFF_STAT_CD,
       DATA_ORIGIN,
       row_number() over (partition by EMPLID, INSTITUTION, STDNT_GROUP, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_STDNT_GRPS_HIST
 where DATA_ORIGIN <> 'D'
   and STDNT_GROUP in ('SRAT','SRHN')),
       Q2 as (
select /*+ INLINE parallel(8) no_merge */
       INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, PERSON_ID, SRC_SYS_ID,
       sum(ENROLL_CNT) TOT_ENRL_CNT
  from CSMRT_OWNER.UM_F_CLASS_ENRLMT
 group by INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, PERSON_ID, SRC_SYS_ID
having sum(ENROLL_CNT) > 0),
       Q3 as (
select /*+ INLINE parallel(8) no_merge */
       INSTITUTION_CD, PERSON_ID, SRC_SYS_ID,
       min(TERM_CD||ACAD_CAR_CD) MIN_TERM_CAR
  from Q2
 group by INSTITUTION_CD, PERSON_ID, SRC_SYS_ID),
       Q4 as (
select /*+ INLINE parallel(8)  */
       CAMPUS, PERSON_SID_BDL
  from HRMRT_OWNER.D_IS_INTERNATIONAL I,
       HRMRT_OWNER.D_IS_SNAPSHOT_CONTROL C
 where I.SNAPSHOT_DATE = C.SNAPSHOT_DATE
   and C.CURRENT_FLAG = 'Y'
   and I.CAMPUS in ('-','UMBOS','UMDAR','UMLOW')
   and PERSON_SID_BDL <> -1),
       Q5 as (
select /*+ inline parallel(8)  */        -- Feb 2020
       EMPLID PERSON_ID, SRC_SYS_ID,
       EXTERNAL_SYSTEM_ID BEACON_CARD_ID,
       row_number() over (partition by EMPLID, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_EXTERNAL_SYSTEM
 where DATA_ORIGIN <> 'D'
   and EXTERNAL_SYSTEM = 'BBC'),
       Q6 as (
select /*+ inline parallel(8)  */
       PERSON_SID, INSTITUTION_CD,
       row_number() over (partition by PERSON_SID
                              order by EMPL_RCD desc) Q_ORDER
  from CSMRT_OWNER.UM_D_PERSON_ACCOM
 where DATA_ORIGIN <> 'D'),
       Q7 as (
select /*+ inline parallel(8) no_merge */
       PERSON_SID, SRC_SYS_ID,
       row_number() over (partition by PERSON_SID, SRC_SYS_ID
                              order by EFFDT desc) Q_ORDER
  from CSMRT_OWNER.UM_D_PERSON_ADDR
 where DATA_ORIGIN <> 'D'),
       Q8 as (
select /*+ inline parallel(8)  */
       PERSON_SID, INSTITUTION_CD,
       row_number() over (partition by PERSON_SID, INSTITUTION_CD
                              order by EFFDT desc) Q_ORDER
  from CSMRT_OWNER.UM_D_PERSON_ATHL
 where DATA_ORIGIN <> 'D'),
       Q9 as (
select /*+ inline parallel(8) no_merge */
       PERSON_SID, INSTITUTION_CD,
       row_number() over (partition by PERSON_SID, INSTITUTION_CD
                              order by EFFDT desc) Q_ORDER
  from CSMRT_OWNER.UM_D_STDNT_ATTR_VAL
 where DATA_ORIGIN <> 'D'),
       Q10 as (
select /*+ inline parallel(8) no_merge */
       PERSON_SID,
       row_number() over (partition by PERSON_SID
                              order by COUNTRY desc) Q_ORDER
  from CSMRT_OWNER.UM_D_PERSON_CITIZEN
 where DATA_ORIGIN <> 'D'),
       Q11 as (
select /*+ inline parallel(8) no_merge */
       PERSON_SID, INSTITUTION_CD,
       row_number() over (partition by PERSON_SID, INSTITUTION_CD
                              order by E_ADDR_TYPE desc) Q_ORDER
  from CSMRT_OWNER.UM_D_PERSON_EMAIL
 where DATA_ORIGIN <> 'D'),
       Q12 as (
select /*+ inline parallel(8) no_merge */
       PERSON_SID,
       row_number() over (partition by PERSON_SID
                              order by ETHNIC_GRP_CD desc) Q_ORDER
  from CSMRT_OWNER.UM_D_PERSON_ETHNICITY
 where DATA_ORIGIN <> 'D'),
       Q13 as (
select /*+ inline parallel(8)  */
       PERSON_SID, INSTITUTION_CD,
       row_number() over (partition by PERSON_SID, INSTITUTION_CD
                              order by ACAD_YEAR_LD desc) Q_ORDER
  from CSMRT_OWNER.UM_F_HOUSING
 where DATA_ORIGIN <> 'D'),
       Q14 as (
select /*+ inline parallel(8) no_merge */
       PERSON_SID,
       row_number() over (partition by PERSON_SID
                              order by NAME_TYPE desc) Q_ORDER
  from CSMRT_OWNER.UM_D_PERSON_NAME
 where DATA_ORIGIN <> 'D'),
       Q15 as (
select /*+ inline parallel(8) no_merge */
       PERSON_SID,
       row_number() over (partition by PERSON_SID
                              order by PHONE_TYPE desc) Q_ORDER
  from CSMRT_OWNER.UM_D_PERSON_PHONE
 where DATA_ORIGIN <> 'D'),
       Q16 as (
select /*+ inline parallel(8) no_merge */
       PERSON_SID, -- INSTITUTION_CD,
       row_number() over (partition by PERSON_SID --, INSTITUTION_CD
                              order by EFF_TERM_CD desc) Q_ORDER
  from CSMRT_OWNER.UM_R_PERSON_RSDNCY
 where DATA_ORIGIN <> 'D'),
       Q17 as (
select /*+ inline parallel(8) no_merge */
       PERSON_SID, --INSTITUTION_CD,
       row_number() over (partition by PERSON_SID --, INSTITUTION_CD
                              order by SRVC_IND_DTTM desc) Q_ORDER
  from CSMRT_OWNER.UM_D_PERSON_SRVC_IND
 where DATA_ORIGIN <> 'D'),
       Q18 as (
select /*+ inline parallel(8) no_merge */
       PERSON_SID,
       row_number() over (partition by PERSON_SID
                              order by COUNTRY desc) Q_ORDER
  from CSMRT_OWNER.UM_D_PERSON_VISA
 where DATA_ORIGIN <> 'D'),
       X as (
select /*+ inline parallel(8) no_merge */
       FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID,
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN,
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D')
select /*+ parallel(8) */
       P.INSTITUTION_CD,
       P.PERSON_ID,
       P.SRC_SYS_ID,
       P.PERSON_SID,
       (nvl(decode(Q1.EFF_STAT_CD,'A','Y','I','N','N'),'-')) CURRENT_ATHL_FLG,
       (nvl(decode(Q2.EFF_STAT_CD,'A','Y','I','N','N'),'-')) CURRENT_HONORS_FLG,
       (nvl(decode(P.INSTITUTION_CD,'UMBOS',Q5.BEACON_CARD_ID,'-'),'-')) BEACON_CARD_ID,     -- Feb 2020
       (nvl(T.TERM_SID,2147483646)) ENRLMT_MIN_PERSON_TERM_SID,
       (nvl(AC.PERSON_SID,2147483646)) PERSON_ACCOM_SID,
       (nvl(AD.PERSON_SID,2147483646)) PERSON_ADDR_SID,
       (nvl(AT.PERSON_SID,2147483646)) PERSON_ATHL_SID,
       (nvl(AR.PERSON_SID,2147483646)) PERSON_ATTRIBUTE_SID,
       (nvl(CT.PERSON_SID,2147483646)) PERSON_CITIZEN_SID,
       (nvl(EM.PERSON_SID,2147483646)) PERSON_EMAIL_SID,
       (nvl(ET.PERSON_SID,2147483646)) PERSON_ETHNICITY_SID,
       (nvl(HS.PERSON_SID,2147483646)) PERSON_HOUSING_SID,       -- Apr 2020
       (nvl(INTL.PERSON_SID_BDL,2147483646)) PERSON_INTL_SID,    -- Dec 2019
       (nvl(NM.PERSON_SID,2147483646)) PERSON_NAME_SID,
       (nvl(PH.PERSON_SID,2147483646)) PERSON_PHONE_SID,
       (nvl(RS.PERSON_SID,2147483646)) PERSON_RSDNCY_SID,        -- Sept 2019
       (nvl(SV.PERSON_SID,2147483646)) PERSON_SRVC_IND_SID,
       (nvl(VI.PERSON_SID,2147483646)) PERSON_VISA_SID,
       'S' DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM
  from Q0 P
  left outer join Q1
    on P.INSTITUTION_CD = Q1.INSTITUTION_CD
   and P.PERSON_ID = Q1.PERSON_ID
   and P.SRC_SYS_ID = Q1.SRC_SYS_ID
   and Q1.Q_ORDER = 1
   and Q1.STDNT_GRP_CD = 'SRAT'
  left outer join Q1 Q2
    on P.INSTITUTION_CD = Q2.INSTITUTION_CD
   and P.PERSON_ID = Q2.PERSON_ID
   and P.SRC_SYS_ID = Q2.SRC_SYS_ID
   and Q2.Q_ORDER = 1
   and Q2.STDNT_GRP_CD = 'SRHN'
  left outer join Q3
    on P.INSTITUTION_CD = Q3.INSTITUTION_CD
   and P.PERSON_ID = Q3.PERSON_ID
   and P.SRC_SYS_ID = Q3.SRC_SYS_ID
  left outer join PS_D_TERM T
    on Q3.INSTITUTION_CD = T.INSTITUTION_CD
   and trim(substr(Q3.MIN_TERM_CAR,5,4)) = T.ACAD_CAR_CD
   and substr(Q3.MIN_TERM_CAR,1,4) = T.TERM_CD
   and Q3.SRC_SYS_ID = T.SRC_SYS_ID
  left outer join Q4 INTL
    on P.INSTITUTION_CD = INTL.CAMPUS                       -- Dec 2019
   and P.PERSON_SID = INTL.PERSON_SID_BDL
  left outer join Q5                                        -- Feb 2020
    on P.PERSON_ID = Q5.PERSON_ID
   and P.SRC_SYS_ID = Q5.SRC_SYS_ID
   and Q5.Q_ORDER = 1
  left outer join Q6 AC                                     -- 12K with no deleted rows
    on P.INSTITUTION_CD = AC.INSTITUTION_CD
   and P.PERSON_SID = AC.PERSON_SID
   and AC.Q_ORDER = 1
  left outer join Q7 AD                                     -- 6M rows with no deleted rows
    on P.PERSON_SID = AD.PERSON_SID
   and AD.Q_ORDER = 1
  left outer join Q8 AT                                   -- 8K with deleted rows
    on P.INSTITUTION_CD = AT.INSTITUTION_CD
   and P.PERSON_SID = AT.PERSON_SID
   and AT.Q_ORDER = 1
  left outer join Q9 AR                                     -- 1.1M with deleted rows
    on P.INSTITUTION_CD = AR.INSTITUTION_CD
   and P.PERSON_SID = AR.PERSON_SID
   and AR.Q_ORDER = 1
  left outer join Q10 CT                                    -- 2M with no deleted rows
    on P.PERSON_SID = CT.PERSON_SID
   and CT.Q_ORDER = 1
  left outer join Q11 EM                                    -- 4.5M with deleted rows
    on P.INSTITUTION_CD = EM.INSTITUTION_CD
   and P.PERSON_SID = EM.PERSON_SID
   and EM.Q_ORDER = 1
  left outer join Q12 ET                                    -- 2.5M with deleted rows
    on P.PERSON_SID = ET.PERSON_SID
   and ET.Q_ORDER = 1
  left outer join Q13 HS                                    -- Apr 2020
    on P.INSTITUTION_CD = HS.INSTITUTION_CD
   and P.PERSON_SID = HS.PERSON_SID
   and HS.Q_ORDER = 1
  left outer join Q14 NM                                    -- 4M with deleted rows
    on P.PERSON_SID = NM.PERSON_SID
   and NM.Q_ORDER = 1
  left outer join Q15 PH                                    -- 2.3M with deleted rows
    on P.PERSON_SID = PH.PERSON_SID
   and PH.Q_ORDER = 1
  left outer join Q16 RS                                    -- Sept 2019
    on P.PERSON_SID = RS.PERSON_SID
--   and P.INSTITUTION_CD = RS.INSTITUTION_CD
   and RS.Q_ORDER = 1
  left outer join Q17 SV                                    -- 650K with no deleted rows
    on P.PERSON_SID = SV.PERSON_SID
--   and P.INSTITUTION_CD = SV.INSTITUTION_CD
   and SV.Q_ORDER = 1
  left outer join Q18 VI                                    -- 2M with deleted rows
    on P.PERSON_SID = VI.PERSON_SID
   and VI.Q_ORDER = 1
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_R_PERSON_ASSOC rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_R_PERSON_ASSOC',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_R_PERSON_ASSOC';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_R_PERSON_ASSOC enable constraint PK_UM_R_PERSON_ASSOC';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_R_PERSON_ASSOC');

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

END UM_R_PERSON_ASSOC_P;
/
