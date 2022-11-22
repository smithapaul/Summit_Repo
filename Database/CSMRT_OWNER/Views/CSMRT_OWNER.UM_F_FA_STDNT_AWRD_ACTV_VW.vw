DROP VIEW CSMRT_OWNER.UM_F_FA_STDNT_AWRD_ACTV_VW
/

--
-- UM_F_FA_STDNT_AWRD_ACTV_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_F_FA_STDNT_AWRD_ACTV_VW
BEQUEATH DEFINER
AS 
SELECT INSTITUTION_CD,
           ACAD_CAR_CD,
           AID_YEAR,
           PERSON_ID,
           ITEM_TYPE,
           ACTION_DTTM                                           AWRD_ACTV_DTTM,
           SRC_SYS_ID,
           TO_CHAR (ACTION_DTTM, 'YYYY-MM-DD HH24:MI:SS.FF')     ACTION_DTTM,
           TRUNC (ACTION_DTTM)                                   ACTION_DT,
           INSTITUTION_SID,
           ACAD_CAR_SID,
           PERSON_SID,
           ITEM_TYPE_SID,
           DISBURSEMENT_PLAN,
           SPLIT_CODE,
           DISBURSEMENT_ID,
           OPRID,
           AWARD_DISB_ACTION,
           AWARD_DISB_ACTION_LD,
           OFFER_AMOUNT,
           ACCEPT_AMOUNT,
           AUTHORIZED_AMOUNT,
           DISB_AMOUNT,
           CURRENCY_CD,
           BUSINESS_UNIT,
           ADJUST_REASON_CD,
           ADJUST_REASON_LD,
           ADJUST_AMOUNT,
           LOAN_ADJUST_CD,
           LOAN_ADJUST_LD,
           DISB_TO_DATE,
           AUTH_TO_DATE,
           PKG_APP_DATA_USED,
           LOAD_ERROR,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM,
           BATCH_SID
      FROM CSMRT_OWNER.UM_F_FA_STDNT_AWRD_ACTV
     WHERE ROWNUM < 100000000
/
