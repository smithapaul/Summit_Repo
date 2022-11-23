DROP INDEX CSMRT_OWNER.AK_UM_S_RA_DATA1
/

--
-- AK_UM_S_RA_DATA1  (Index) 
--
CREATE INDEX CSMRT_OWNER.AK_UM_S_RA_DATA1 ON CSMRT_OWNER.UM_S_RA_DATA
(RUN_DT)
  LOCAL (  
  PARTITION P201709
    NOCOMPRESS ,  
  PARTITION P201710
    NOCOMPRESS ,  
  PARTITION P201711
    NOCOMPRESS ,  
  PARTITION P201712
    NOCOMPRESS ,  
  PARTITION P201801
    NOCOMPRESS ,  
  PARTITION P201802
    NOCOMPRESS ,  
  PARTITION P201803
    NOCOMPRESS ,  
  PARTITION P201804
    NOCOMPRESS ,  
  PARTITION P201805
    NOCOMPRESS ,  
  PARTITION P201806
    NOCOMPRESS ,  
  PARTITION P201807
    NOCOMPRESS ,  
  PARTITION P201808
    NOCOMPRESS ,  
  PARTITION P201809
    NOCOMPRESS ,  
  PARTITION P201810
    NOCOMPRESS ,  
  PARTITION P201811
    NOCOMPRESS ,  
  PARTITION P201812
    NOCOMPRESS ,  
  PARTITION P201901
    NOCOMPRESS ,  
  PARTITION P201902
    NOCOMPRESS ,  
  PARTITION P201903
    NOCOMPRESS ,  
  PARTITION P201904
    NOCOMPRESS ,  
  PARTITION P201905
    NOCOMPRESS ,  
  PARTITION P201906
    NOCOMPRESS ,  
  PARTITION P201907
    NOCOMPRESS ,  
  PARTITION P201908
    NOCOMPRESS ,  
  PARTITION P201909
    NOCOMPRESS ,  
  PARTITION P201910
    NOCOMPRESS ,  
  PARTITION P201911
    NOCOMPRESS ,  
  PARTITION P201912
    NOCOMPRESS ,  
  PARTITION P202001
    NOCOMPRESS ,  
  PARTITION P202002
    NOCOMPRESS ,  
  PARTITION P202003
    NOCOMPRESS ,  
  PARTITION P202004
    NOCOMPRESS ,  
  PARTITION P202005
    NOCOMPRESS ,  
  PARTITION P202006
    NOCOMPRESS ,  
  PARTITION P202007
    NOCOMPRESS ,  
  PARTITION P202008
    NOCOMPRESS ,  
  PARTITION P202009
    NOCOMPRESS ,  
  PARTITION P202010
    NOCOMPRESS ,  
  PARTITION P202011
    NOCOMPRESS ,  
  PARTITION P202012
    NOCOMPRESS ,  
  PARTITION P202101
    NOCOMPRESS ,  
  PARTITION P202102
    NOCOMPRESS ,  
  PARTITION P202103
    NOCOMPRESS ,  
  PARTITION P202104
    NOCOMPRESS ,  
  PARTITION P202105
    NOCOMPRESS ,  
  PARTITION P202106
    NOCOMPRESS ,  
  PARTITION P202107
    NOCOMPRESS ,  
  PARTITION P202108
    NOCOMPRESS ,  
  PARTITION P202109
    NOCOMPRESS ,  
  PARTITION P202110
    NOCOMPRESS ,  
  PARTITION P202111
    NOCOMPRESS ,  
  PARTITION P202112
    NOCOMPRESS ,  
  PARTITION P202201
    NOCOMPRESS ,  
  PARTITION P202202
    NOCOMPRESS ,  
  PARTITION P202203
    NOCOMPRESS ,  
  PARTITION P202204
    NOCOMPRESS ,  
  PARTITION P202205
    NOCOMPRESS ,  
  PARTITION P202206
    NOCOMPRESS ,  
  PARTITION P202207
    NOCOMPRESS ,  
  PARTITION P202208
    NOCOMPRESS ,  
  PARTITION P202209
    NOCOMPRESS ,  
  PARTITION P202210
    NOCOMPRESS ,  
  PARTITION P202211
    NOCOMPRESS ,  
  PARTITION P202212
    NOCOMPRESS ,  
  PARTITION P202301
    NOCOMPRESS ,  
  PARTITION P202302
    NOCOMPRESS ,  
  PARTITION P202303
    NOCOMPRESS ,  
  PARTITION P202304
    NOCOMPRESS ,  
  PARTITION P202305
    NOCOMPRESS ,  
  PARTITION P202306
    NOCOMPRESS ,  
  PARTITION P202307
    NOCOMPRESS ,  
  PARTITION P202308
    NOCOMPRESS ,  
  PARTITION P202309
    NOCOMPRESS ,  
  PARTITION P202310
    NOCOMPRESS ,  
  PARTITION P202311
    NOCOMPRESS ,  
  PARTITION P202312
    NOCOMPRESS
)
/
