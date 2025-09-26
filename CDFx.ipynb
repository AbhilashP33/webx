#1) Join tool usage and label
# --- c360_detail + tracking_tool_use -> TOOL_USED (SAS 'Tool Used' / 'Tool Not Used') ---
c360_detail = c360_detail_pre.copy()

# Align join key if needed
if 'OPPOR_ID' not in c360_detail.columns and 'RBC_OPPOR_OWN_ID' in c360_detail.columns:
    c360_detail = c360_detail.rename(columns={'RBC_OPPOR_OWN_ID': 'OPPOR_ID'})

# Safe empty frame for join
if 'OPPOR_ID' not in globals() or tracking_tool_use is None:
    tracking_tool_use = pd.DataFrame(columns=['OPPOR_ID', 'tool_used'])

# Left join
if not c360_detail.empty and 'OPPOR_ID' in c360_detail.columns:
    c360_detail = c360_detail.merge(
        tracking_tool_use[['OPPOR_ID', 'tool_used']].drop_duplicates('OPPOR_ID'),
        how='left', on='OPPOR_ID'
    )
    c360_detail['TOOL_USED'] = c360_detail['tool_used'].apply(
        lambda v: 'Tool Used' if pd.notna(v) and str(v).strip() != '' else 'Tool Not Used'
    )
    if 'tool_used' in c360_detail.columns:
        c360_detail = c360_detail.drop(columns=['tool_used'])
# 2) Stage mapping (SAS $stagefmt.)
stagefmt_map = {
    "Démarche exploratoire/Comprendre le besoin": "11.Démarche exploratoire/Comprendre le besoin",
    "Discovery/Understand Needs": "12.Discovery/Understand Needs",
    "Review Options": "21.Review Options",
    "Present/Gain Commitment": "31.Present/Gain Commitment",
    "Intégration commencée": "41.Intégration commencée",
    "Onboarding Started": "42.Onboarding Started",
    "Opportunity Lost": "51.Opportunity Lost",
    "Opportunity Won": "61.Opportunity Won",
}

if 'oppor_stage_nm' in c360_detail.columns:
    c360_detail['stage_fmt'] = c360_detail['oppor_stage_nm'].map(stagefmt_map).fillna(c360_detail['oppor_stage_nm'])
# 3) Build C360_PDA_Link_AOT exactly like SAS
# aot_all_oppor: counts per oppor_id within the date window (already filtered upstream in SAS)
# Here we only need the distinct oppor_id list like SAS aot_all_oppor_unique
if 'aot_all_oppor' in globals() and not aot_all_oppor.empty:
    aot_all_oppor_unique = aot_all_oppor[['OPPOR_ID']].dropna().drop_duplicates()
else:
    aot_all_oppor_unique = pd.DataFrame(columns=['OPPOR_ID'])

# Left join & flag
if not c360_detail.empty and 'OPPOR_ID' in c360_detail.columns:
    c360_detail = c360_detail.merge(aot_all_oppor_unique.assign(_has_aot=1),
                                    how='left', on='OPPOR_ID')
    c360_detail['_has_aot'] = c360_detail['_has_aot'].fillna(0).astype(int)

    c360_detail['C360_PDA_Link_AOT'] = np.where(
        (c360_detail.get('PROD_CATG_NM', '').eq('Personal Accounts')) &
        (c360_detail['_has_aot'].eq(1)),
        1, 0
    )
    c360_detail = c360_detail.drop(columns=['_has_aot'], errors='ignore')
# 4) PRE cohort filter & “more” outputs (mirrors SAS output logic)

# SAS:
# if (asct_prod_fmly_nm ^= 'Risk Protection') & (lob = 'Retail') & (C360_PDA_Link_AOT = 0)
#    & (oppor_stage_nm in ('Opportunity Won','Opportunity Lost')) then output c360_detail_more_in_pre;
# output c360_detail_more;

c360_detail_more = c360_detail.copy()

mask = pd.Series(True, index=c360_detail_more.index)
if 'ASCT_PROD_FMLY_NM' in c360_detail_more.columns:
    mask &= c360_detail_more['ASCT_PROD_FMLY_NM'].ne('Risk Protection')
if 'lob' in c360_detail_more.columns:
    mask &= c360_detail_more['lob'].eq('Retail')
if 'C360_PDA_Link_AOT' in c360_detail_more.columns:
    mask &= c360_detail_more['C360_PDA_Link_AOT'].fillna(0).astype(int).eq(0)
if 'oppor_stage_nm' in c360_detail_more.columns:
    mask &= c360_detail_more['oppor_stage_nm'].isin(['Opportunity Won', 'Opportunity Lost'])

c360_detail_more_in_pre = c360_detail_more.loc[mask].copy()
# 5) PA rationale normalization & validation (bit-for-bit with SAS intent)
# Select rows with PA rationale reason
if not c360_detail_more_in_pre.empty and 'IS_PROD_APRP_FOR_CLNT' in c360_detail_more_in_pre.columns:
    pa_mask = c360_detail_more_in_pre['IS_PROD_APRP_FOR_CLNT'].eq('Not Appropriate - Rationale')
    req = ['EVNT_ID', 'IS_PROD_APRP_FOR_CLNT', 'CLNT_RTNL_TXT']
    have = [c for c in req if c in c360_detail_more_in_pre.columns]
    pa_rationale = c360_detail_more_in_pre.loc[pa_mask, have].copy() if len(have) == len(req) else pd.DataFrame(columns=req)
else:
    pa_rationale = pd.DataFrame(columns=['EVNT_ID','IS_PROD_APRP_FOR_CLNT','CLNT_RTNL_TXT'])

# SAS-like normalization: collapse whitespace to single blank, strip, uppercase
_ws_re = re.compile(r'\s+')
def normalize_sas(txt):
    if pd.isna(txt):
        return ''
    s = str(txt)
    s = _ws_re.sub(' ', s)     # uniform whitespace to single blank (SAS translate/compress)
    s = s.strip().upper()      # strip + upcase
    return s

def is_valid_sas(s):
    # Assumes s is already normalized as SAS 'x'
    if not s:
        return False
    # (1) length(x) > 5  (spaces count, as in SAS)
    if len(s) <= 5:
        return False
    # (2) cannot be only repeated characters
    first = s[0]
    # Remove all occurrences of the first character (SAS: compress(x, substrn(x,1,1)))
    remain = ''.join(ch for ch in s if ch != first)
    if remain == '':
        return False
    # (3) have at least 2 alphanumeric characters (SAS: compress(x,'a','kad') length >= 2)
    alnum_count = sum(ch.isalnum() for ch in s)
    if alnum_count < 2:
        return False
    return True

if not pa_rationale.empty:
    pa_rationale['rationale_clean'] = pa_rationale['CLNT_RTNL_TXT'].apply(normalize_sas)
    pa_rationale['is_valid_rationale'] = pa_rationale['rationale_clean'].apply(is_valid_sas)
    pa_rationale['prod_not_appr_rtnl_txt_cat'] = np.where(
        pa_rationale['is_valid_rationale'], 'Valid', 'Invalid'
    )
# 6) BY-group counter = SAS level_oppor
# After you produce c360_detail_more_in (or any table that needs the counter):
tmp = c360_detail_more_in_pre.sort_values(['OPPOR_ID']).copy()
if 'OPPOR_ID' in tmp.columns:
    tmp['level_oppor'] = tmp.groupby('OPPOR_ID').cumcount()
# replace back or keep as needed
c360_detail_more_in_pre = tmp
# 7) (Optional) $cs_cmt format as a mapping
cs_cmt_map = {
    'COM1': 'Test population (less samples)',
    'COM2': 'Match population',
    'COM3': 'Mismatch population (less samples)',
    'COM4': 'Non Anomaly Population',
    'COM5': 'Anomaly Population',
    'COM6': 'Number of Deposit Sessions',
    'COM7': 'Number of Accounts',
    'COM8': 'Number of Transactions',
    'COM9': 'Non Blank Population',
    'COM10': 'Blank Population',
    'COM11': 'Unable to Assess',
    'COM12': 'Number of Failed Data Elements',
    'COM13': 'Population Distribution',
    'COM14': 'Reconciled Population',
    'COM15': 'Not Reconciled Population',
    'COM16': 'Pass',
    'COM17': 'Fail',
    'COM18': 'Not Applicable',
    'COM19': 'Potential Fail',
}
# Example usage:
# df['CS_CMT_DESC'] = df['CS_CMT'].map(cs_cmt_map).fillna(df['CS_CMT'])
