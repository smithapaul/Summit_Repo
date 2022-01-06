CREATE OR REPLACE PROCEDURE             "UM_D_CLASS_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--OLD tables               -- UM_D_CLASS /UM_D_CLASS_VW
--Loads target table       -- UM_D_CLASS
--UM_D_CLASS           --Dependent on PS_D_ACAD_CAR  ,PS_D_ACAD_GRP  ,PS_D_ACAD_ORG  ,PS_D_CAMPUS  ,
                           --UM_D_CLASS_ASSOC  ,PS_D_INSTITUTION  ,PS_D_INSTRCTN_MODE  ,PS_D_LOCATION  ,PS_D_SESSION  ,
						   --PS_D_TERM  ,UM_D_CRSE , UM_D_SCTN_CMBND , PS_D_SSR_COMP
-- V01 4/16/2018           -- srikanth ,pabbu converted to proc from sql
-- V02 3/4/2021 Case 53679 -- Jim  Added ONLINE_FLG
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_CLASS';
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

strMessage01    := 'Merging data into CSMRT_OWNER.UM_D_CLASS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.UM_D_CLASS';
merge /* parallel(8) */ /*+ use_hash(S,T) */ into CSMRT_OWNER.UM_D_CLASS T
using (
with
SUB as ( 
select /*+ PARALLEL(8) INLINE */ 
       INSTITUTION, SUBJECT, EFFDT, SRC_SYS_ID, 
       EFF_STATUS, DESCR, DESCRSHORT, DESCRFORMAL,
       row_number() over (partition by INSTITUTION, SUBJECT, SRC_SYS_ID
                              order by decode(EFF_STATUS,'I',9,0), EFFDT desc) SUB_ORDER 
  from CSSTG_OWNER.PS_SUBJECT_TBL 
 where DATA_ORIGIN <> 'D'),
CMB as (
select /*+ PARALLEL(8) INLINE */  
       INSTITUTION, STRM, SESSION_CODE, CLASS_NBR, SRC_SYS_ID, 
       SCTN_COMBINED_ID
  from CSSTG_OWNER.PS_SCTN_CMBND S
 where DATA_ORIGIN <> 'D'),
       X as (  
select /*+ inline */ 
       FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
SRC as ( 
select /*+ PARALLEL(8) */ 
       CLS.CRSE_ID CRSE_CD,
       CLS.CRSE_OFFER_NBR CRSE_OFFER_NUM,
       CLS.STRM TERM_CD,
       CLS.SESSION_CODE SESSION_CD,
       CLS.CLASS_SECTION CLASS_SECTION_CD,
       CLS.SRC_SYS_ID,
       CLS.INSTITUTION INSTITUTION_CD,
       CLS.CLASS_NBR CLASS_NUM,
       I.INSTITUTION_SID, 
       to_char(CLS.CLASS_NBR) CLASS_CD,
       CLS.DESCR, 
       nvl(C.ACAD_CAR_SID,2147483646) ACAD_CAR_SID, 
       nvl(T.TERM_SID,2147483646) TERM_SID, 
       nvl(R.CRSE_SID,2147483646) CRSE_SID, 
       nvl(S.SESSION_SID,2147483646) SESSION_SID, 
       nvl(G.ACAD_GRP_SID,2147483646) ACAD_GRP_SID, 
       nvl(O.ACAD_ORG_SID,2147483646) ACAD_ORG_SID, 
       nvl(M.CAMPUS_SID,2147483646) CAMPUS_SID, 
       nvl(SOC.CLASS_ASSOC_SID,2147483646) CLASS_ASSOC_SID, 
       nvl(MOD.INSTRCTN_MODE_SID,2147483646) INSTRCTN_MODE_SID, 
       nvl(LOC.LOC_SID,2147483646) LOC_SID, 
       nvl(SC.SCTN_COMBINED_SID,2147483646) SCTN_COMBINED_SID, 
       nvl(SSR.SSR_COMP_SID,2147483646) SSR_COMP_SID, 
       CLS.ATTEND_GENERATE ATTEND_GENERATE_FLG,
       CLS.ATTEND_SYNC_REQD ATTEND_SYNC_REQD_FLG,
       (CASE WHEN CLS.INSTITUTION = 'UMDAR'               -- Case: 52679,   March 2021
              AND CLS.CLASS_SECTION BETWEEN '7100'  AND '7199'
             THEN 'Y'
             WHEN CLS.INSTITUTION <> 'UMDAR'
              AND MOD.INSTRCTN_MODE_CD IN ('OL','OS','WH')
             THEN 'Y'
            ELSE 'N'
       END) ONLINE_FLG,
       CLS.AUTO_ENROLL_SECT_1,
       CLS.AUTO_ENROLL_SECT_2,
       CLS.AUTO_ENRL_WAITLIST AUTO_ENRL_WAITLIST_FLG,
       CLS.CAMPUS_EVENT_NBR,
       trim(CLS.CATALOG_NBR) CATALOG_NBR,
          (CASE WHEN CLS.INSTITUTION = 'UMLOW'
                 AND CLS.STRM >= '2510'
                 AND SUBSTR (trim(CLS.CATALOG_NBR), 1, 1) BETWEEN '0' AND '1'
            THEN '1000 (Lower)'
            WHEN CLS.INSTITUTION = 'UMLOW'
             AND CLS.STRM >= '2510'
             AND SUBSTR (trim(CLS.CATALOG_NBR), 1, 1) BETWEEN '2' AND '2'
            THEN '2000 (Lower)'
            WHEN CLS.INSTITUTION = 'UMLOW'
             AND CLS.STRM >= '2510'
             AND SUBSTR (trim(CLS.CATALOG_NBR), 1, 1) BETWEEN '3' AND '3'
            THEN '3000 (Upper)'
            WHEN CLS.INSTITUTION = 'UMLOW'
             AND CLS.STRM >= '2510'
             AND SUBSTR (trim(CLS.CATALOG_NBR), 1, 1) BETWEEN '4' AND '4'
            THEN '4000 (Upper)'
            WHEN CLS.INSTITUTION = 'UMLOW'
             AND CLS.STRM >= '2510'
             AND SUBSTR (trim(CLS.CATALOG_NBR), 1, 1) BETWEEN '5' AND '5'
            THEN '5000 (Graduate)'
            WHEN CLS.INSTITUTION = 'UMLOW'
             AND CLS.STRM >= '2510'
             AND SUBSTR (trim(CLS.CATALOG_NBR), 1, 1) BETWEEN '6' AND '6'
            THEN'6000 (Graduate)'
            WHEN CLS.INSTITUTION = 'UMLOW'
             AND CLS.STRM >= '2510'
             AND SUBSTR (trim(CLS.CATALOG_NBR), 1, 1) BETWEEN '7' AND '7'
            THEN '7000 (Graduate)'
            WHEN CLS.INSTITUTION = 'UMLOW'
             AND CLS.STRM >= '2510'
             AND SUBSTR (trim(CLS.CATALOG_NBR), 1, 1) BETWEEN '8' AND '8'
            THEN '8000 (Graduate)'
            WHEN CLS.INSTITUTION = 'UMLOW'
             AND CLS.STRM >= '2510'
             AND SUBSTR (trim(CLS.CATALOG_NBR), 1, 1) BETWEEN '9' AND '9'
            THEN '9000 (Other)'
            WHEN CLS.INSTITUTION = 'UMLOW'
             AND CLS.STRM >= '2510'
             AND SUBSTR (trim(CLS.CATALOG_NBR), 1, 1) NOT BETWEEN '0' AND '9'
            THEN '9000 (Other)'
            WHEN SUBSTR (trim(CLS.CATALOG_NBR), 1, 1) BETWEEN '0' AND '1'
            THEN '100 (Lower)'
            WHEN SUBSTR (trim(CLS.CATALOG_NBR), 1, 1) BETWEEN '2' AND '2'
            THEN '200 (Lower)'
            WHEN SUBSTR (trim(CLS.CATALOG_NBR), 1, 1) BETWEEN '3' AND '3'
            THEN '300 (Upper)'
            WHEN SUBSTR (trim(CLS.CATALOG_NBR), 1, 1) BETWEEN '4' AND '4'
            THEN '400 (Upper)'
            WHEN SUBSTR (trim(CLS.CATALOG_NBR), 1, 1) BETWEEN '5' AND '5'
            THEN '500 (Graduate)'
            WHEN SUBSTR (trim(CLS.CATALOG_NBR), 1, 1) BETWEEN '6' AND '6'
            THEN '600 (Graduate)'
            WHEN SUBSTR (trim(CLS.CATALOG_NBR), 1, 1) BETWEEN '7' AND '7'
            THEN '700 (Graduate)'
            WHEN SUBSTR (trim(CLS.CATALOG_NBR), 1, 1) BETWEEN '8' AND '8'
            THEN '800 (Graduate)'
            WHEN SUBSTR (trim(CLS.CATALOG_NBR), 1, 1) BETWEEN '9' AND '9'
            THEN '900 (Other)'
            ELSE '900 (Other)'
        END) CRSE_LVL,
       CLS.START_DT CLASS_START_DT, 
       CLS.END_DT CLASS_END_DT, 
       CLS.CANCEL_DT CLASS_CANCEL_DATE, 
       CLS.CLASS_STAT,
       nvl(X1.XLATSHORTNAME,'-') CLASS_STAT_SD,
       nvl(X1.XLATLONGNAME,'-') CLASS_STAT_LD,
       CLS.CLASS_TYPE,
       nvl(X2.XLATSHORTNAME,'-') CLASS_TYPE_SD,
       nvl(X2.XLATLONGNAME,'-') CLASS_TYPE_LD,
       CLS.CNCL_IF_STUD_ENRLD CNCL_IF_STUD_ENRLD_FLG,
       CLS.COMBINED_SECTION,
       X3.XLATSHORTNAME COMBINED_SECTION_SD,
       X3.XLATLONGNAME COMBINED_SECTION_LD,
       CLS.CONSENT,
       nvl(X4.XLATSHORTNAME,'-') CONSENT_SD,
       nvl(X4.XLATLONGNAME,'-') CONSENT_LD,
       CLS.CRS_TOPIC_ID,
       CLS.DYN_DT_INCLUDE DYN_DT_INCLUDE_FLG,
       CLS.DYN_DT_CALC_REQ DYN_DT_CALC_REQ_FLG,
       CLS.ENRL_STAT,
       nvl(X5.XLATSHORTNAME,'-') ENRL_STAT_SD,
       nvl(X5.XLATLONGNAME,'-') ENRL_STAT_LD,
       CLS.FEES_EXIST FEES_EXIST_FLG,
       CLS.HOLIDAY_SCHEDULE HOLIDAY_SCHED_CD,
       '-' HOLIDAY_SCHED_SD, 
       '-' HOLIDAY_SCHED_LD, 
       CLS.NEXT_STDNT_POSITIN,
       CLS.PRIM_INSTR_SECT,
       CLS.PRINT_TOPIC PRINT_TOPIC_FLG,
       CLS.RCV_FROM_ITEM_TYPE RCV_FROM_ITEM_TYPE_FLG,
       CLS.SUBJECT SBJCT_CD,
       nvl(SUB.DESCRSHORT,'-') SBJCT_SD, 
       nvl(SUB.DESCR,'-') SBJCT_LD, 
       nvl(SUB.DESCRFORMAL,'-') SBJCT_FD,
       CLS.SCHEDULE_PRINT SCHEDULE_PRINT_FLG,
       nvl(CLS.SSR_DROP_CONSENT,'-') SSR_DROP_CONSENT,
       nvl(X7.XLATSHORTNAME,'-') SSR_DROP_CONSENT_SD,
       nvl(X7.XLATLONGNAME,'-') SSR_DROP_CONSENT_LD,
       CLS.STDNT_SPEC_PERM STDNT_SPEC_PERM_FLG,
	   T.TERM_BEGIN_DT, 
	   T.TERM_END_DT, 
       CLS.WAITLIST_DAEMON,
       nvl(X6.XLATSHORTNAME,'-') WAITLIST_DAEMON_SD,
       nvl(X6.XLATLONGNAME,'-') WAITLIST_DAEMON_LD,
       CLS.ENRL_CAP,
       CLS.ENRL_TOT,
       CLS.EXAM_SEAT_SPACING,
       CLS.MIN_ENRL,
       CLS.ROOM_CAP_REQUEST,
       CLS.WAIT_CAP,
       CLS.WAIT_TOT,
       'N' LOAD_ERROR, 
       CLS.DATA_ORIGIN,
       SYSDATE CREATED_EW_DTTM, 
       SYSDATE LASTUPD_EW_DTTM, 
       1234 BATCH_SID
  from CSSTG_OWNER.PS_CLASS_TBL CLS
  join PS_D_INSTITUTION I  
    on CLS.INSTITUTION = I.INSTITUTION_CD
   and CLS.SRC_SYS_ID = I.SRC_SYS_ID
  left outer join SUB
    on CLS.INSTITUTION = SUB.INSTITUTION  
   and CLS.SUBJECT = SUB.SUBJECT  
   and CLS.SRC_SYS_ID = SUB.SRC_SYS_ID  
   and SUB.SUB_ORDER = 1 
  left outer join CMB
    on CLS.INSTITUTION = CMB.INSTITUTION
   and CLS.STRM = CMB.STRM
   and CLS.SESSION_CODE = CMB.SESSION_CODE
   and CLS.CLASS_NBR = CMB.CLASS_NBR
   and CLS.SRC_SYS_ID = CMB.SRC_SYS_ID
  join PS_D_ACAD_CAR C  
    on CLS.INSTITUTION = C.INSTITUTION_CD
   and CLS.ACAD_CAREER = C.ACAD_CAR_CD
   and CLS.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  join PS_D_TERM T  
    on CLS.INSTITUTION = T.INSTITUTION_CD
   and CLS.ACAD_CAREER = T.ACAD_CAR_CD
   and CLS.STRM = T.TERM_CD
   and CLS.SRC_SYS_ID = T.SRC_SYS_ID
   and T.DATA_ORIGIN <> 'D'
  left outer join UM_D_CRSE R  
    on CLS.CRSE_ID = R.CRSE_CD
   and CLS.CRSE_OFFER_NBR = R.CRSE_OFFER_NUM
   and CLS.SRC_SYS_ID = R.SRC_SYS_ID
   and R.DATA_ORIGIN <> 'D'
  join PS_D_SESSION S  
    on CLS.INSTITUTION = S.INSTITUTION_CD 
   and CLS.ACAD_CAREER = S.ACAD_CAR_CD
   and CLS.STRM = S.TERM_CD
   and CLS.SESSION_CODE = S.SESSION_CD
   and CLS.SRC_SYS_ID = S.SRC_SYS_ID
   and S.DATA_ORIGIN <> 'D'
  left outer join PS_D_ACAD_GRP G  
    on CLS.INSTITUTION = G.INSTITUTION_CD 
   and CLS.ACAD_GROUP= G.ACAD_GRP_CD 
   and CLS.SRC_SYS_ID = G.SRC_SYS_ID
   and G.EFFDT_ORDER = 1
   and G.DATA_ORIGIN <> 'D'
  left outer join PS_D_ACAD_ORG O  
    on CLS.INSTITUTION = O.INSTITUTION_CD 
   and CLS.ACAD_ORG= O.ACAD_ORG_CD 
   and CLS.SRC_SYS_ID = O.SRC_SYS_ID
   and O.EFFDT_ORDER = 1
   and O.DATA_ORIGIN <> 'D'
  left outer join PS_D_CAMPUS M  
    on CLS.INSTITUTION = M.INSTITUTION_CD 
   and CLS.CAMPUS= M.CAMPUS_CD 
   and CLS.SRC_SYS_ID = M.SRC_SYS_ID
   and M.DATA_ORIGIN <> 'D'
  left outer join UM_D_CLASS_ASSOC SOC  
    on CLS.CRSE_ID = SOC.CRSE_CD
   and CLS.CRSE_OFFER_NBR = SOC.CRSE_OFFER_NUM
   and CLS.STRM = SOC.TERM_CD
   and CLS.SESSION_CODE = SOC.SESSION_CD
   and CLS.ASSOCIATED_CLASS = SOC.ASSOCIATED_CLASS
   and CLS.SRC_SYS_ID = SOC.SRC_SYS_ID
   and SOC.DATA_ORIGIN <> 'D'
  left outer join PS_D_INSTRCTN_MODE MOD  
    on CLS.INSTITUTION = MOD.INSTITUTION_CD 
   and CLS.INSTRUCTION_MODE = MOD.INSTRCTN_MODE_CD 
   and CLS.SRC_SYS_ID = MOD.SRC_SYS_ID
   and MOD.DATA_ORIGIN <> 'D'
  left outer join PS_D_LOCATION LOC  
    on CLS.INSTITUTION = LOC.SETID 
   and CLS.LOCATION = LOC.LOC_ID 
   and CLS.SRC_SYS_ID = LOC.SRC_SYS_ID
   and LOC.DATA_ORIGIN <> 'D'
  left outer join UM_D_SCTN_CMBND SC  
    on CLS.INSTITUTION = SC.INSTITUTION_CD 
   and CLS.STRM = SC.TERM_CD
   and CLS.SESSION_CODE = SC.SESSION_CD
   and nvl(CMB.SCTN_COMBINED_ID,'-') = SC.SCTN_COMBINED_ID
   and CLS.SRC_SYS_ID = SC.SRC_SYS_ID
   and SC.DATA_ORIGIN <> 'D'
  left outer join PS_D_SSR_COMP SSR  
    on CLS.SSR_COMPONENT = SSR.SSR_COMP_CD 
   and CLS.SRC_SYS_ID = SSR.SRC_SYS_ID
   and SSR.DATA_ORIGIN <> 'D'
  left outer join X X1
    on CLS.CLASS_STAT = X1.FIELDVALUE
   and CLS.SRC_SYS_ID = X1.SRC_SYS_ID
   and X1.FIELDNAME = 'CLASS_STAT'
   and X1.X_ORDER = 1  
  left outer join X X2
    on CLS.CLASS_TYPE = X2.FIELDVALUE
   and CLS.SRC_SYS_ID = X2.SRC_SYS_ID
   and X2.FIELDNAME = 'CLASS_TYPE'
   and X2.X_ORDER = 1  
  left outer join X X3
    on CLS.COMBINED_SECTION = X3.FIELDVALUE
   and CLS.SRC_SYS_ID = X3.SRC_SYS_ID
   and X3.FIELDNAME = 'COMBINED_SECTION'
   and X3.X_ORDER = 1  
  left outer join X X4
    on CLS.CONSENT = X4.FIELDVALUE
   and CLS.SRC_SYS_ID = X4.SRC_SYS_ID
   and X4.FIELDNAME = 'CONSENT'
   and X4.X_ORDER = 1  
  left outer join X X5
    on CLS.ENRL_STAT = X5.FIELDVALUE
   and CLS.SRC_SYS_ID = X5.SRC_SYS_ID
   and X5.FIELDNAME = 'ENRL_STAT'
   and X5.X_ORDER = 1  
  left outer join X X6
    on CLS.WAITLIST_DAEMON = X6.FIELDVALUE
   and CLS.SRC_SYS_ID = X6.SRC_SYS_ID
   and X6.FIELDNAME = 'WAITLIST_DAEMON'
   and X6.X_ORDER = 1  
  left outer join X X7
    on CLS.SSR_DROP_CONSENT = X7.FIELDVALUE
   and CLS.SRC_SYS_ID = X7.SRC_SYS_ID
   and X7.FIELDNAME = 'SSR_DROP_CONSENT'
   and X7.X_ORDER = 1  
)
select  nvl(TGT.CLASS_SID, 
       (select nvl(max(CLASS_SID),0) from CSMRT_OWNER.UM_D_CLASS where CLASS_SID < 2147483646) + 
              row_number() over (partition by 1 order by TGT.CLASS_SID nulls first)) CLASS_SID, 
       nvl(TGT.CRSE_CD, SRC.CRSE_CD) CRSE_CD, 
       nvl(TGT.CRSE_OFFER_NUM, SRC.CRSE_OFFER_NUM) CRSE_OFFER_NUM, 
       nvl(TGT.TERM_CD, SRC.TERM_CD) TERM_CD, 
       nvl(TGT.SESSION_CD, SRC.SESSION_CD) SESSION_CD, 
       nvl(TGT.CLASS_SECTION_CD, SRC.CLASS_SECTION_CD) CLASS_SECTION_CD, 
       nvl(TGT.SRC_SYS_ID, SRC.SRC_SYS_ID) SRC_SYS_ID, 
       decode(TGT.INSTITUTION_CD, SRC.INSTITUTION_CD, TGT.INSTITUTION_CD, SRC.INSTITUTION_CD) INSTITUTION_CD, 
       decode(TGT.CLASS_NUM, SRC.CLASS_NUM, TGT.CLASS_NUM, SRC.CLASS_NUM) CLASS_NUM, 
       decode(TGT.INSTITUTION_SID, SRC.INSTITUTION_SID, TGT.INSTITUTION_SID, SRC.INSTITUTION_SID) INSTITUTION_SID, 
       decode(TGT.CLASS_CD, SRC.CLASS_CD, TGT.CLASS_CD, SRC.CLASS_CD) CLASS_CD, 
       decode(TGT.DESCR, SRC.DESCR, TGT.DESCR, SRC.DESCR) DESCR, 
       decode(TGT.ACAD_CAR_SID, SRC.ACAD_CAR_SID, TGT.ACAD_CAR_SID, SRC.ACAD_CAR_SID) ACAD_CAR_SID, 
       decode(TGT.TERM_SID, SRC.TERM_SID, TGT.TERM_SID, SRC.TERM_SID) TERM_SID, 
       decode(TGT.CRSE_SID, SRC.CRSE_SID, TGT.CRSE_SID, SRC.CRSE_SID) CRSE_SID, 
       decode(TGT.SESSION_SID, SRC.SESSION_SID, TGT.SESSION_SID, SRC.SESSION_SID) SESSION_SID, 
       decode(TGT.ACAD_GRP_SID, SRC.ACAD_GRP_SID, TGT.ACAD_GRP_SID, SRC.ACAD_GRP_SID) ACAD_GRP_SID, 
       decode(TGT.ACAD_ORG_SID, SRC.ACAD_ORG_SID, TGT.ACAD_ORG_SID, SRC.ACAD_ORG_SID) ACAD_ORG_SID, 
       decode(TGT.CAMPUS_SID, SRC.CAMPUS_SID, TGT.CAMPUS_SID, SRC.CAMPUS_SID) CAMPUS_SID, 
       decode(TGT.CLASS_ASSOC_SID, SRC.CLASS_ASSOC_SID, TGT.CLASS_ASSOC_SID, SRC.CLASS_ASSOC_SID) CLASS_ASSOC_SID, 
       decode(TGT.INSTRCTN_MODE_SID, SRC.INSTRCTN_MODE_SID, TGT.INSTRCTN_MODE_SID, SRC.INSTRCTN_MODE_SID) INSTRCTN_MODE_SID, 
       decode(TGT.LOC_SID, SRC.LOC_SID, TGT.LOC_SID, SRC.LOC_SID) LOC_SID, 
       decode(TGT.SCTN_COMBINED_SID, SRC.SCTN_COMBINED_SID, TGT.SCTN_COMBINED_SID, SRC.SCTN_COMBINED_SID) SCTN_COMBINED_SID, 
       decode(TGT.SSR_COMP_SID, SRC.SSR_COMP_SID, TGT.SSR_COMP_SID, SRC.SSR_COMP_SID) SSR_COMP_SID, 
       decode(TGT.ATTEND_GENERATE_FLG, SRC.ATTEND_GENERATE_FLG, TGT.ATTEND_GENERATE_FLG, SRC.ATTEND_GENERATE_FLG) ATTEND_GENERATE_FLG, 
       decode(TGT.ATTEND_SYNC_REQD_FLG, SRC.ATTEND_SYNC_REQD_FLG, TGT.ATTEND_SYNC_REQD_FLG, SRC.ATTEND_SYNC_REQD_FLG) ATTEND_SYNC_REQD_FLG, 
       decode(TGT.ONLINE_FLG, SRC.ONLINE_FLG, TGT.ONLINE_FLG, SRC.ONLINE_FLG) ONLINE_FLG,   -- Case: 52679,   March 2021
       decode(TGT.AUTO_ENROLL_SECT_1, SRC.AUTO_ENROLL_SECT_1, TGT.AUTO_ENROLL_SECT_1, SRC.AUTO_ENROLL_SECT_1) AUTO_ENROLL_SECT_1, 
       decode(TGT.AUTO_ENROLL_SECT_2, SRC.AUTO_ENROLL_SECT_2, TGT.AUTO_ENROLL_SECT_2, SRC.AUTO_ENROLL_SECT_2) AUTO_ENROLL_SECT_2, 
       decode(TGT.AUTO_ENRL_WAITLIST_FLG, SRC.AUTO_ENRL_WAITLIST_FLG, TGT.AUTO_ENRL_WAITLIST_FLG, SRC.AUTO_ENRL_WAITLIST_FLG) AUTO_ENRL_WAITLIST_FLG, 
       decode(TGT.CAMPUS_EVENT_NBR, SRC.CAMPUS_EVENT_NBR, TGT.CAMPUS_EVENT_NBR, SRC.CAMPUS_EVENT_NBR) CAMPUS_EVENT_NBR, 
       decode(TGT.CATALOG_NBR, SRC.CATALOG_NBR, TGT.CATALOG_NBR, SRC.CATALOG_NBR) CATALOG_NBR, 
       decode(TGT.CRSE_LVL, SRC.CRSE_LVL, TGT.CRSE_LVL, SRC.CRSE_LVL) CRSE_LVL, 
       decode(TGT.CLASS_START_DT, SRC.CLASS_START_DT, TGT.CLASS_START_DT, SRC.CLASS_START_DT) CLASS_START_DT, 
       decode(TGT.CLASS_END_DT, SRC.CLASS_END_DT, TGT.CLASS_END_DT, SRC.CLASS_END_DT) CLASS_END_DT, 
       decode(TGT.CLASS_CANCEL_DATE, SRC.CLASS_CANCEL_DATE, TGT.CLASS_CANCEL_DATE, SRC.CLASS_CANCEL_DATE) CLASS_CANCEL_DATE, 
       decode(TGT.CLASS_STAT, SRC.CLASS_STAT, TGT.CLASS_STAT, SRC.CLASS_STAT) CLASS_STAT, 
       decode(TGT.CLASS_STAT_SD, SRC.CLASS_STAT_SD, TGT.CLASS_STAT_SD, SRC.CLASS_STAT_SD) CLASS_STAT_SD, 
       decode(TGT.CLASS_STAT_LD, SRC.CLASS_STAT_LD, TGT.CLASS_STAT_LD, SRC.CLASS_STAT_LD) CLASS_STAT_LD, 
       decode(TGT.CLASS_TYPE, SRC.CLASS_TYPE, TGT.CLASS_TYPE, SRC.CLASS_TYPE) CLASS_TYPE, 
       decode(TGT.CLASS_TYPE_SD, SRC.CLASS_TYPE_SD, TGT.CLASS_TYPE_SD, SRC.CLASS_TYPE_SD) CLASS_TYPE_SD, 
       decode(TGT.CLASS_TYPE_LD, SRC.CLASS_TYPE_LD, TGT.CLASS_TYPE_LD, SRC.CLASS_TYPE_LD) CLASS_TYPE_LD, 
       decode(TGT.CNCL_IF_STUD_ENRLD_FLG, SRC.CNCL_IF_STUD_ENRLD_FLG, TGT.CNCL_IF_STUD_ENRLD_FLG, SRC.CNCL_IF_STUD_ENRLD_FLG) CNCL_IF_STUD_ENRLD_FLG, 
       decode(TGT.COMBINED_SECTION, SRC.COMBINED_SECTION, TGT.COMBINED_SECTION, SRC.COMBINED_SECTION) COMBINED_SECTION, 
       decode(TGT.COMBINED_SECTION_SD, SRC.COMBINED_SECTION_SD, TGT.COMBINED_SECTION_SD, SRC.COMBINED_SECTION_SD) COMBINED_SECTION_SD, 
       decode(TGT.COMBINED_SECTION_LD, SRC.COMBINED_SECTION_LD, TGT.COMBINED_SECTION_LD, SRC.COMBINED_SECTION_LD) COMBINED_SECTION_LD, 
       decode(TGT.CONSENT, SRC.CONSENT, TGT.CONSENT, SRC.CONSENT) CONSENT, 
       decode(TGT.CONSENT_SD, SRC.CONSENT_SD, TGT.CONSENT_SD, SRC.CONSENT_SD) CONSENT_SD, 
       decode(TGT.CONSENT_LD, SRC.CONSENT_LD, TGT.CONSENT_LD, SRC.CONSENT_LD) CONSENT_LD, 
       decode(TGT.CRS_TOPIC_ID, SRC.CRS_TOPIC_ID, TGT.CRS_TOPIC_ID, SRC.CRS_TOPIC_ID) CRS_TOPIC_ID, 
       decode(TGT.DYN_DT_INCLUDE_FLG, SRC.DYN_DT_INCLUDE_FLG, TGT.DYN_DT_INCLUDE_FLG, SRC.DYN_DT_INCLUDE_FLG) DYN_DT_INCLUDE_FLG, 
       decode(TGT.DYN_DT_CALC_REQ_FLG, SRC.DYN_DT_CALC_REQ_FLG, TGT.DYN_DT_CALC_REQ_FLG, SRC.DYN_DT_CALC_REQ_FLG) DYN_DT_CALC_REQ_FLG, 
       decode(TGT.ENRL_STAT, SRC.ENRL_STAT, TGT.ENRL_STAT, SRC.ENRL_STAT) ENRL_STAT, 
       decode(TGT.ENRL_STAT_SD, SRC.ENRL_STAT_SD, TGT.ENRL_STAT_SD, SRC.ENRL_STAT_SD) ENRL_STAT_SD, 
       decode(TGT.ENRL_STAT_LD, SRC.ENRL_STAT_LD, TGT.ENRL_STAT_LD, SRC.ENRL_STAT_LD) ENRL_STAT_LD, 
       decode(TGT.FEES_EXIST_FLG, SRC.FEES_EXIST_FLG, TGT.FEES_EXIST_FLG, SRC.FEES_EXIST_FLG) FEES_EXIST_FLG, 
       decode(TGT.HOLIDAY_SCHED_CD, SRC.HOLIDAY_SCHED_CD, TGT.HOLIDAY_SCHED_CD, SRC.HOLIDAY_SCHED_CD) HOLIDAY_SCHED_CD, 
       decode(TGT.HOLIDAY_SCHED_SD, SRC.HOLIDAY_SCHED_SD, TGT.HOLIDAY_SCHED_SD, SRC.HOLIDAY_SCHED_SD) HOLIDAY_SCHED_SD, 
       decode(TGT.HOLIDAY_SCHED_LD, SRC.HOLIDAY_SCHED_LD, TGT.HOLIDAY_SCHED_LD, SRC.HOLIDAY_SCHED_LD) HOLIDAY_SCHED_LD, 
       decode(TGT.NEXT_STDNT_POSITIN, SRC.NEXT_STDNT_POSITIN, TGT.NEXT_STDNT_POSITIN, SRC.NEXT_STDNT_POSITIN) NEXT_STDNT_POSITIN, 
       decode(TGT.PRIM_INSTR_SECT, SRC.PRIM_INSTR_SECT, TGT.PRIM_INSTR_SECT, SRC.PRIM_INSTR_SECT) PRIM_INSTR_SECT, 
       decode(TGT.PRINT_TOPIC_FLG, SRC.PRINT_TOPIC_FLG, TGT.PRINT_TOPIC_FLG, SRC.PRINT_TOPIC_FLG) PRINT_TOPIC_FLG, 
       decode(TGT.RCV_FROM_ITEM_TYPE_FLG, SRC.RCV_FROM_ITEM_TYPE_FLG, TGT.RCV_FROM_ITEM_TYPE_FLG, SRC.RCV_FROM_ITEM_TYPE_FLG) RCV_FROM_ITEM_TYPE_FLG, 
       decode(TGT.SBJCT_CD, SRC.SBJCT_CD, TGT.SBJCT_CD, SRC.SBJCT_CD) SBJCT_CD, 
       decode(TGT.SBJCT_SD, SRC.SBJCT_SD, TGT.SBJCT_SD, SRC.SBJCT_SD) SBJCT_SD, 
       decode(TGT.SBJCT_LD, SRC.SBJCT_LD, TGT.SBJCT_LD, SRC.SBJCT_LD) SBJCT_LD, 
       decode(TGT.SBJCT_FD, SRC.SBJCT_FD, TGT.SBJCT_FD, SRC.SBJCT_FD) SBJCT_FD, 
       decode(TGT.SCHEDULE_PRINT_FLG, SRC.SCHEDULE_PRINT_FLG, TGT.SCHEDULE_PRINT_FLG, SRC.SCHEDULE_PRINT_FLG) SCHEDULE_PRINT_FLG, 
       decode(TGT.SSR_DROP_CONSENT, SRC.SSR_DROP_CONSENT, TGT.SSR_DROP_CONSENT, SRC.SSR_DROP_CONSENT) SSR_DROP_CONSENT, 
       decode(TGT.SSR_DROP_CONSENT_SD, SRC.SSR_DROP_CONSENT_SD, TGT.SSR_DROP_CONSENT_SD, SRC.SSR_DROP_CONSENT_SD) SSR_DROP_CONSENT_SD, 
       decode(TGT.SSR_DROP_CONSENT_LD, SRC.SSR_DROP_CONSENT_LD, TGT.SSR_DROP_CONSENT_LD, SRC.SSR_DROP_CONSENT_LD) SSR_DROP_CONSENT_LD, 
       decode(TGT.STDNT_SPEC_PERM_FLG, SRC.STDNT_SPEC_PERM_FLG, TGT.STDNT_SPEC_PERM_FLG, SRC.STDNT_SPEC_PERM_FLG) STDNT_SPEC_PERM_FLG, 
       decode(TGT.TERM_BEGIN_DT, SRC.TERM_BEGIN_DT, TGT.TERM_BEGIN_DT, SRC.TERM_BEGIN_DT) TERM_BEGIN_DT, 
       decode(TGT.TERM_END_DT, SRC.TERM_END_DT, TGT.TERM_END_DT, SRC.TERM_END_DT) TERM_END_DT, 
       decode(TGT.WAITLIST_DAEMON, SRC.WAITLIST_DAEMON, TGT.WAITLIST_DAEMON, SRC.WAITLIST_DAEMON) WAITLIST_DAEMON, 
       decode(TGT.WAITLIST_DAEMON_SD, SRC.WAITLIST_DAEMON_SD, TGT.WAITLIST_DAEMON_SD, SRC.WAITLIST_DAEMON_SD) WAITLIST_DAEMON_SD, 
       decode(TGT.WAITLIST_DAEMON_LD, SRC.WAITLIST_DAEMON_LD, TGT.WAITLIST_DAEMON_LD, SRC.WAITLIST_DAEMON_LD) WAITLIST_DAEMON_LD, 
       decode(TGT.ENRL_CAP, SRC.ENRL_CAP, TGT.ENRL_CAP, SRC.ENRL_CAP) ENRL_CAP, 
       decode(TGT.ENRL_TOT, SRC.ENRL_TOT, TGT.ENRL_TOT, SRC.ENRL_TOT) ENRL_TOT, 
       decode(TGT.EXAM_SEAT_SPACING, SRC.EXAM_SEAT_SPACING, TGT.EXAM_SEAT_SPACING, SRC.EXAM_SEAT_SPACING) EXAM_SEAT_SPACING, 
       decode(TGT.MIN_ENRL, SRC.MIN_ENRL, TGT.MIN_ENRL, SRC.MIN_ENRL) MIN_ENRL, 
       decode(TGT.ROOM_CAP_REQUEST, SRC.ROOM_CAP_REQUEST, TGT.ROOM_CAP_REQUEST, SRC.ROOM_CAP_REQUEST) ROOM_CAP_REQUEST, 
       decode(TGT.WAIT_CAP, SRC.WAIT_CAP, TGT.WAIT_CAP, SRC.WAIT_CAP) WAIT_CAP, 
       decode(TGT.WAIT_TOT, SRC.WAIT_TOT, TGT.WAIT_TOT, SRC.WAIT_TOT) WAIT_TOT, 
       decode(TGT.DATA_ORIGIN, SRC.DATA_ORIGIN, TGT.DATA_ORIGIN, SRC.DATA_ORIGIN) DATA_ORIGIN,     -- Sept 2016 
       nvl(TGT.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM, 
       nvl(TGT.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM 
  from SRC
  left outer join CSMRT_OWNER.UM_D_CLASS TGT 
    on SRC.CRSE_CD = TGT.CRSE_CD
   and SRC.CRSE_OFFER_NUM = TGT.CRSE_OFFER_NUM
   and SRC.TERM_CD = TGT.TERM_CD
   and SRC.SESSION_CD = TGT.SESSION_CD
   and SRC.CLASS_SECTION_CD = TGT.CLASS_SECTION_CD
   and SRC.SRC_SYS_ID = TGT.SRC_SYS_ID
   and TGT.CLASS_SID < 2147483646
) S
    on (S.CRSE_CD = T.CRSE_CD
   and  S.CRSE_OFFER_NUM = T.CRSE_OFFER_NUM
   and  S.TERM_CD = T.TERM_CD
   and  S.SESSION_CD = T.SESSION_CD
   and  S.CLASS_SECTION_CD = T.CLASS_SECTION_CD
   and  S.SRC_SYS_ID = T.SRC_SYS_ID)  -- Need 'CS90'??? 
  when matched then update set 
       T.INSTITUTION_CD = S.INSTITUTION_CD, 
       T.CLASS_NUM = S.CLASS_NUM, 
       T.INSTITUTION_SID = S.INSTITUTION_SID, 
       T.CLASS_CD = S.CLASS_CD, 
       T.DESCR = S.DESCR, 
       T.ACAD_CAR_SID = S.ACAD_CAR_SID, 
       T.TERM_SID = S.TERM_SID, 
       T.CRSE_SID = S.CRSE_SID, 
       T.SESSION_SID = S.SESSION_SID, 
       T.ACAD_GRP_SID = S.ACAD_GRP_SID, 
       T.ACAD_ORG_SID = S.ACAD_ORG_SID, 
       T.CAMPUS_SID = S.CAMPUS_SID, 
       T.CLASS_ASSOC_SID = S.CLASS_ASSOC_SID, 
       T.INSTRCTN_MODE_SID = S.INSTRCTN_MODE_SID, 
       T.LOC_SID = S.LOC_SID, 
       T.SCTN_COMBINED_SID = S.SCTN_COMBINED_SID, 
       T.SSR_COMP_SID = S.SSR_COMP_SID, 
       T.ATTEND_GENERATE_FLG = S.ATTEND_GENERATE_FLG, 
       T.ATTEND_SYNC_REQD_FLG = S.ATTEND_SYNC_REQD_FLG, 
	   T.ONLINE_FLG = S.ONLINE_FLG,                         -- Case: 52679,   March 2021
       T.AUTO_ENROLL_SECT_1 = S.AUTO_ENROLL_SECT_1, 
       T.AUTO_ENROLL_SECT_2 = S.AUTO_ENROLL_SECT_2, 
       T.AUTO_ENRL_WAITLIST_FLG = S.AUTO_ENRL_WAITLIST_FLG, 
       T.CAMPUS_EVENT_NBR = S.CAMPUS_EVENT_NBR, 
       T.CATALOG_NBR = S.CATALOG_NBR, 
       T.CRSE_LVL = S.CRSE_LVL, 
       T.CLASS_START_DT = S.CLASS_START_DT, 
       T.CLASS_END_DT = S.CLASS_END_DT, 
       T.CLASS_CANCEL_DATE = S.CLASS_CANCEL_DATE, 
       T.CLASS_STAT = S.CLASS_STAT, 
       T.CLASS_STAT_SD = S.CLASS_STAT_SD, 
       T.CLASS_STAT_LD = S.CLASS_STAT_LD, 
       T.CLASS_TYPE = S.CLASS_TYPE, 
       T.CLASS_TYPE_SD = S.CLASS_TYPE_SD, 
       T.CLASS_TYPE_LD = S.CLASS_TYPE_LD, 
       T.CNCL_IF_STUD_ENRLD_FLG = S.CNCL_IF_STUD_ENRLD_FLG, 
       T.COMBINED_SECTION = S.COMBINED_SECTION, 
       T.COMBINED_SECTION_SD = S.COMBINED_SECTION_SD, 
       T.COMBINED_SECTION_LD = S.COMBINED_SECTION_LD, 
       T.CONSENT = S.CONSENT, 
       T.CONSENT_SD = S.CONSENT_SD, 
       T.CONSENT_LD = S.CONSENT_LD, 
       T.CRS_TOPIC_ID = S.CRS_TOPIC_ID, 
       T.DYN_DT_INCLUDE_FLG = S.DYN_DT_INCLUDE_FLG, 
       T.DYN_DT_CALC_REQ_FLG = S.DYN_DT_CALC_REQ_FLG, 
       T.ENRL_STAT = S.ENRL_STAT, 
       T.ENRL_STAT_SD = S.ENRL_STAT_SD, 
       T.ENRL_STAT_LD = S.ENRL_STAT_LD, 
       T.FEES_EXIST_FLG = S.FEES_EXIST_FLG, 
       T.HOLIDAY_SCHED_CD = S.HOLIDAY_SCHED_CD, 
       T.HOLIDAY_SCHED_SD = S.HOLIDAY_SCHED_SD, 
       T.HOLIDAY_SCHED_LD = S.HOLIDAY_SCHED_LD, 
       T.NEXT_STDNT_POSITIN = S.NEXT_STDNT_POSITIN, 
       T.PRIM_INSTR_SECT = S.PRIM_INSTR_SECT, 
       T.PRINT_TOPIC_FLG = S.PRINT_TOPIC_FLG, 
       T.RCV_FROM_ITEM_TYPE_FLG = S.RCV_FROM_ITEM_TYPE_FLG, 
       T.SBJCT_CD = S.SBJCT_CD, 
       T.SBJCT_SD = S.SBJCT_SD, 
       T.SBJCT_LD = S.SBJCT_LD, 
       T.SBJCT_FD = S.SBJCT_FD, 
       T.SCHEDULE_PRINT_FLG = S.SCHEDULE_PRINT_FLG, 
       T.SSR_DROP_CONSENT = S.SSR_DROP_CONSENT, 
       T.SSR_DROP_CONSENT_SD = S.SSR_DROP_CONSENT_SD, 
       T.SSR_DROP_CONSENT_LD = S.SSR_DROP_CONSENT_LD, 
       T.STDNT_SPEC_PERM_FLG = S.STDNT_SPEC_PERM_FLG, 
       T.TERM_BEGIN_DT = S.TERM_BEGIN_DT, 
       T.TERM_END_DT = S.TERM_END_DT, 
       T.WAITLIST_DAEMON = S.WAITLIST_DAEMON, 
       T.WAITLIST_DAEMON_SD = S.WAITLIST_DAEMON_SD, 
       T.WAITLIST_DAEMON_LD = S.WAITLIST_DAEMON_LD, 
       T.ENRL_CAP = S.ENRL_CAP, 
       T.ENRL_TOT = S.ENRL_TOT, 
       T.EXAM_SEAT_SPACING = S.EXAM_SEAT_SPACING, 
       T.MIN_ENRL = S.MIN_ENRL, 
       T.ROOM_CAP_REQUEST = S.ROOM_CAP_REQUEST, 
       T.WAIT_CAP = S.WAIT_CAP, 
       T.WAIT_TOT = S.WAIT_TOT, 
       T.DATA_ORIGIN = S.DATA_ORIGIN, 
       T.LASTUPD_EW_DTTM = SYSDATE
 where decode(T.INSTITUTION_CD, S.INSTITUTION_CD, 0, 1) = 1 or 
       decode(T.CLASS_NUM, S.CLASS_NUM, 0, 1) = 1 or 
       decode(T.INSTITUTION_SID, S.INSTITUTION_SID, 0, 1) = 1 or 
       decode(T.CLASS_CD, S.CLASS_CD, 0, 1) = 1 or 
       decode(T.DESCR, S.DESCR, 0, 1) = 1 or 
       decode(T.ACAD_CAR_SID, S.ACAD_CAR_SID, 0, 1) = 1 or 
       decode(T.TERM_SID, S.TERM_SID, 0, 1) = 1 or 
       decode(T.CRSE_SID, S.CRSE_SID, 0, 1) = 1 or 
       decode(T.SESSION_SID, S.SESSION_SID, 0, 1) = 1 or 
       decode(T.ACAD_GRP_SID, S.ACAD_GRP_SID, 0, 1) = 1 or 
       decode(T.ACAD_ORG_SID, S.ACAD_ORG_SID, 0, 1) = 1 or 
       decode(T.CAMPUS_SID, S.CAMPUS_SID, 0, 1) = 1 or 
       decode(T.CLASS_ASSOC_SID, S.CLASS_ASSOC_SID, 0, 1) = 1 or 
       decode(T.INSTRCTN_MODE_SID, S.INSTRCTN_MODE_SID, 0, 1) = 1 or 
       decode(T.LOC_SID, S.LOC_SID, 0, 1) = 1 or 
       decode(T.SCTN_COMBINED_SID, S.SCTN_COMBINED_SID, 0, 1) = 1 or 
       decode(T.SSR_COMP_SID, S.SSR_COMP_SID, 0, 1) = 1 or 
       decode(T.ATTEND_GENERATE_FLG, S.ATTEND_GENERATE_FLG, 0, 1) = 1 or 
       decode(T.ATTEND_SYNC_REQD_FLG, S.ATTEND_SYNC_REQD_FLG, 0, 1) = 1 or 
	   decode(T.ONLINE_FLG, S.ONLINE_FLG, 0, 1) = 1 or                 -- Case: 52679,   March 2021
       decode(T.AUTO_ENROLL_SECT_1, S.AUTO_ENROLL_SECT_1, 0, 1) = 1 or 
       decode(T.AUTO_ENROLL_SECT_2, S.AUTO_ENROLL_SECT_2, 0, 1) = 1 or 
       decode(T.AUTO_ENRL_WAITLIST_FLG, S.AUTO_ENRL_WAITLIST_FLG, 0, 1) = 1 or 
       decode(T.CAMPUS_EVENT_NBR, S.CAMPUS_EVENT_NBR, 0, 1) = 1 or 
       decode(T.CATALOG_NBR, S.CATALOG_NBR, 0, 1) = 1 or 
       decode(T.CRSE_LVL, S.CRSE_LVL, 0, 1) = 1 or 
       decode(T.CLASS_START_DT, S.CLASS_START_DT, 0, 1) = 1 or 
       decode(T.CLASS_END_DT, S.CLASS_END_DT, 0, 1) = 1 or 
       decode(T.CLASS_CANCEL_DATE, S.CLASS_CANCEL_DATE, 0, 1) = 1 or 
       decode(T.CLASS_STAT, S.CLASS_STAT, 0, 1) = 1 or 
       decode(T.CLASS_STAT_SD, S.CLASS_STAT_SD, 0, 1) = 1 or 
       decode(T.CLASS_STAT_LD, S.CLASS_STAT_LD, 0, 1) = 1 or 
       decode(T.CLASS_TYPE, S.CLASS_TYPE, 0, 1) = 1 or 
       decode(T.CLASS_TYPE_SD, S.CLASS_TYPE_SD, 0, 1) = 1 or 
       decode(T.CLASS_TYPE_LD, S.CLASS_TYPE_LD, 0, 1) = 1 or 
       decode(T.CNCL_IF_STUD_ENRLD_FLG, S.CNCL_IF_STUD_ENRLD_FLG, 0, 1) = 1 or 
       decode(T.COMBINED_SECTION, S.COMBINED_SECTION, 0, 1) = 1 or 
       decode(T.COMBINED_SECTION_SD, S.COMBINED_SECTION_SD, 0, 1) = 1 or 
       decode(T.COMBINED_SECTION_LD, S.COMBINED_SECTION_LD, 0, 1) = 1 or 
       decode(T.CONSENT, S.CONSENT, 0, 1) = 1 or 
       decode(T.CONSENT_SD, S.CONSENT_SD, 0, 1) = 1 or 
       decode(T.CONSENT_LD, S.CONSENT_LD, 0, 1) = 1 or 
       decode(T.CRS_TOPIC_ID, S.CRS_TOPIC_ID, 0, 1) = 1 or 
       decode(T.DYN_DT_INCLUDE_FLG, S.DYN_DT_INCLUDE_FLG, 0, 1) = 1 or 
       decode(T.DYN_DT_CALC_REQ_FLG, S.DYN_DT_CALC_REQ_FLG, 0, 1) = 1 or 
       decode(T.ENRL_STAT, S.ENRL_STAT, 0, 1) = 1 or 
       decode(T.ENRL_STAT_SD, S.ENRL_STAT_SD, 0, 1) = 1 or 
       decode(T.ENRL_STAT_LD, S.ENRL_STAT_LD, 0, 1) = 1 or 
       decode(T.FEES_EXIST_FLG, S.FEES_EXIST_FLG, 0, 1) = 1 or 
       decode(T.HOLIDAY_SCHED_CD, S.HOLIDAY_SCHED_CD, 0, 1) = 1 or 
       decode(T.HOLIDAY_SCHED_SD, S.HOLIDAY_SCHED_SD, 0, 1) = 1 or 
       decode(T.HOLIDAY_SCHED_LD, S.HOLIDAY_SCHED_LD, 0, 1) = 1 or 
       decode(T.NEXT_STDNT_POSITIN, S.NEXT_STDNT_POSITIN, 0, 1) = 1 or 
       decode(T.PRIM_INSTR_SECT, S.PRIM_INSTR_SECT, 0, 1) = 1 or 
       decode(T.PRINT_TOPIC_FLG, S.PRINT_TOPIC_FLG, 0, 1) = 1 or 
       decode(T.RCV_FROM_ITEM_TYPE_FLG, S.RCV_FROM_ITEM_TYPE_FLG, 0, 1) = 1 or 
       decode(T.SBJCT_CD, S.SBJCT_CD, 0, 1) = 1 or 
       decode(T.SBJCT_SD, S.SBJCT_SD, 0, 1) = 1 or 
       decode(T.SBJCT_LD, S.SBJCT_LD, 0, 1) = 1 or 
       decode(T.SBJCT_FD, S.SBJCT_FD, 0, 1) = 1 or 
       decode(T.SCHEDULE_PRINT_FLG, S.SCHEDULE_PRINT_FLG, 0, 1) = 1 or 
       decode(T.SSR_DROP_CONSENT, S.SSR_DROP_CONSENT, 0, 1) = 1 or 
       decode(T.SSR_DROP_CONSENT_SD, S.SSR_DROP_CONSENT_SD, 0, 1) = 1 or 
       decode(T.SSR_DROP_CONSENT_LD, S.SSR_DROP_CONSENT_LD, 0, 1) = 1 or 
       decode(T.STDNT_SPEC_PERM_FLG, S.STDNT_SPEC_PERM_FLG, 0, 1) = 1 or 
       decode(T.TERM_BEGIN_DT, S.TERM_BEGIN_DT, 0, 1) = 1 or 
       decode(T.TERM_END_DT, S.TERM_END_DT, 0, 1) = 1 or 
       decode(T.WAITLIST_DAEMON, S.WAITLIST_DAEMON, 0, 1) = 1 or 
       decode(T.WAITLIST_DAEMON_SD, S.WAITLIST_DAEMON_SD, 0, 1) = 1 or 
       decode(T.WAITLIST_DAEMON_LD, S.WAITLIST_DAEMON_LD, 0, 1) = 1 or 
       decode(T.ENRL_CAP, S.ENRL_CAP, 0, 1) = 1 or 
       decode(T.ENRL_TOT, S.ENRL_TOT, 0, 1) = 1 or 
       decode(T.EXAM_SEAT_SPACING, S.EXAM_SEAT_SPACING, 0, 1) = 1 or 
       decode(T.MIN_ENRL, S.MIN_ENRL, 0, 1) = 1 or 
       decode(T.ROOM_CAP_REQUEST, S.ROOM_CAP_REQUEST, 0, 1) = 1 or 
       decode(T.WAIT_CAP, S.WAIT_CAP, 0, 1) = 1 or 
       decode(T.WAIT_TOT, S.WAIT_TOT, 0, 1) = 1 or 
       decode(T.DATA_ORIGIN, S.DATA_ORIGIN, 0, 1) = 1  
  when not matched then
insert (
       T.CLASS_SID, 
       T.CRSE_CD, 
       T.CRSE_OFFER_NUM, 
       T.TERM_CD, 
       T.SESSION_CD, 
       T.CLASS_SECTION_CD, 
       T.SRC_SYS_ID, 
       T.INSTITUTION_CD, 
       T.CLASS_NUM, 
       T.INSTITUTION_SID, 
       T.CLASS_CD, 
       T.DESCR, 
       T.ACAD_CAR_SID, 
       T.TERM_SID, 
       T.CRSE_SID, 
       T.SESSION_SID, 
       T.ACAD_GRP_SID, 
       T.ACAD_ORG_SID, 
       T.CAMPUS_SID, 
       T.CLASS_ASSOC_SID, 
       T.INSTRCTN_MODE_SID, 
       T.LOC_SID, 
       T.SCTN_COMBINED_SID, 
       T.SSR_COMP_SID, 
       T.ATTEND_GENERATE_FLG, 
       T.ATTEND_SYNC_REQD_FLG, 
	   T.ONLINE_FLG,            -- Case: 52679,   March 2021
       T.AUTO_ENROLL_SECT_1, 
       T.AUTO_ENROLL_SECT_2, 
       T.AUTO_ENRL_WAITLIST_FLG, 
       T.CAMPUS_EVENT_NBR, 
       T.CATALOG_NBR, 
       T.CRSE_LVL, 
       T.CLASS_START_DT, 
       T.CLASS_END_DT, 
       T.CLASS_CANCEL_DATE, 
       T.CLASS_STAT, 
       T.CLASS_STAT_SD, 
       T.CLASS_STAT_LD, 
       T.CLASS_TYPE, 
       T.CLASS_TYPE_SD, 
       T.CLASS_TYPE_LD, 
       T.CNCL_IF_STUD_ENRLD_FLG, 
       T.COMBINED_SECTION, 
       T.COMBINED_SECTION_SD, 
       T.COMBINED_SECTION_LD, 
       T.CONSENT, 
       T.CONSENT_SD, 
       T.CONSENT_LD, 
       T.CRS_TOPIC_ID, 
       T.DYN_DT_INCLUDE_FLG, 
       T.DYN_DT_CALC_REQ_FLG, 
       T.ENRL_STAT, 
       T.ENRL_STAT_SD, 
       T.ENRL_STAT_LD, 
       T.FEES_EXIST_FLG, 
       T.HOLIDAY_SCHED_CD, 
       T.HOLIDAY_SCHED_SD, 
       T.HOLIDAY_SCHED_LD, 
       T.NEXT_STDNT_POSITIN, 
       T.PRIM_INSTR_SECT, 
       T.PRINT_TOPIC_FLG, 
       T.RCV_FROM_ITEM_TYPE_FLG, 
       T.SBJCT_CD, 
       T.SBJCT_SD, 
       T.SBJCT_LD, 
       T.SBJCT_FD, 
       T.SCHEDULE_PRINT_FLG, 
       T.SSR_DROP_CONSENT, 
       T.SSR_DROP_CONSENT_SD, 
       T.SSR_DROP_CONSENT_LD, 
       T.STDNT_SPEC_PERM_FLG, 
	   T.TERM_BEGIN_DT, 
	   T.TERM_END_DT, 
       T.WAITLIST_DAEMON, 
       T.WAITLIST_DAEMON_SD, 
       T.WAITLIST_DAEMON_LD, 
       T.ENRL_CAP, 
       T.ENRL_TOT, 
       T.EXAM_SEAT_SPACING, 
       T.MIN_ENRL, 
       T.ROOM_CAP_REQUEST, 
       T.WAIT_CAP, 
       T.WAIT_TOT, 
       T.DATA_ORIGIN, 
       T.CREATED_EW_DTTM, 
       T.LASTUPD_EW_DTTM)
values (
       S.CLASS_SID, 
       S.CRSE_CD, 
       S.CRSE_OFFER_NUM, 
       S.TERM_CD, 
       S.SESSION_CD, 
       S.CLASS_SECTION_CD, 
       S.SRC_SYS_ID, 
       S.INSTITUTION_CD, 
       S.CLASS_NUM, 
       S.INSTITUTION_SID, 
       S.CLASS_CD, 
       S.DESCR, 
       S.ACAD_CAR_SID, 
       S.TERM_SID, 
       S.CRSE_SID, 
       S.SESSION_SID, 
       S.ACAD_GRP_SID, 
       S.ACAD_ORG_SID, 
       S.CAMPUS_SID, 
       S.CLASS_ASSOC_SID, 
       S.INSTRCTN_MODE_SID, 
       S.LOC_SID, 
       S.SCTN_COMBINED_SID, 
       S.SSR_COMP_SID, 
       S.ATTEND_GENERATE_FLG, 
       S.ATTEND_SYNC_REQD_FLG,
       S.ONLINE_FLG,     -- Case: 52679,   March 2021	   
       S.AUTO_ENROLL_SECT_1, 
       S.AUTO_ENROLL_SECT_2, 
       S.AUTO_ENRL_WAITLIST_FLG, 
       S.CAMPUS_EVENT_NBR, 
       S.CATALOG_NBR, 
       S.CRSE_LVL, 
       S.CLASS_START_DT, 
       S.CLASS_END_DT, 
       S.CLASS_CANCEL_DATE, 
       S.CLASS_STAT, 
       S.CLASS_STAT_SD, 
       S.CLASS_STAT_LD, 
       S.CLASS_TYPE, 
       S.CLASS_TYPE_SD, 
       S.CLASS_TYPE_LD, 
       S.CNCL_IF_STUD_ENRLD_FLG, 
       S.COMBINED_SECTION, 
       S.COMBINED_SECTION_SD, 
       S.COMBINED_SECTION_LD, 
       S.CONSENT, 
       S.CONSENT_SD, 
       S.CONSENT_LD, 
       S.CRS_TOPIC_ID, 
       S.DYN_DT_INCLUDE_FLG, 
       S.DYN_DT_CALC_REQ_FLG, 
       S.ENRL_STAT, 
       S.ENRL_STAT_SD, 
       S.ENRL_STAT_LD, 
       S.FEES_EXIST_FLG, 
       S.HOLIDAY_SCHED_CD, 
       S.HOLIDAY_SCHED_SD, 
       S.HOLIDAY_SCHED_LD, 
       S.NEXT_STDNT_POSITIN, 
       S.PRIM_INSTR_SECT, 
       S.PRINT_TOPIC_FLG, 
       S.RCV_FROM_ITEM_TYPE_FLG, 
       S.SBJCT_CD, 
       S.SBJCT_SD, 
       S.SBJCT_LD, 
       S.SBJCT_FD, 
       S.SCHEDULE_PRINT_FLG, 
       S.SSR_DROP_CONSENT, 
       S.SSR_DROP_CONSENT_SD, 
       S.SSR_DROP_CONSENT_LD, 
       S.STDNT_SPEC_PERM_FLG, 
	   S.TERM_BEGIN_DT, 
	   S.TERM_END_DT, 
       S.WAITLIST_DAEMON, 
       S.WAITLIST_DAEMON_SD, 
       S.WAITLIST_DAEMON_LD, 
       S.ENRL_CAP, 
       S.ENRL_TOT, 
       S.EXAM_SEAT_SPACING, 
       S.MIN_ENRL, 
       S.ROOM_CAP_REQUEST, 
       S.WAIT_CAP, 
       S.WAIT_TOT, 
       S.DATA_ORIGIN, 
       SYSDATE, 
       SYSDATE) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_CLASS rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_CLASS',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.UM_D_CLASS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.UM_D_CLASS';

update CSMRT_OWNER.UM_D_CLASS T 
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.CLASS_SID < 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PS_CLASS_TBL S
                    where T.CRSE_CD = S.CRSE_ID
                      and T.CRSE_OFFER_NUM = S.CRSE_OFFER_NBR
                      and T.TERM_CD = S.STRM
                      and T.SESSION_CD = S.SESSION_CODE
                      and T.CLASS_SECTION_CD = S.CLASS_SECTION
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
					  and S.DATA_ORIGIN <> 'D');

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_CLASS rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_CLASS',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
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

END UM_D_CLASS_P;
/
