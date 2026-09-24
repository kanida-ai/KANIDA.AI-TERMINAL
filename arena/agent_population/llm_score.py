"""Score the LLM reader picks vs operator picks + returns. Reads llm_sheets/picks_*.json + ground_truth.json.
recall@15 (LLM picks that match operator), LLM basket next-day intraday return vs operator vs all-candidate baseline."""
import os, json, numpy as np
D=r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\arena\agent_population\llm_sheets"
GT=json.load(open(os.path.join(D,"ground_truth.json")))
print(f"  {'day':<12}{'LLM picks':>10}{'recall@15':>11}{'LLM ret%':>10}{'OP ret%':>9}{'baseline%':>11}{'LLM win%':>10}")
hits=tot=0; llm_rets=[]; op_rets=[]; base_rets=[]
for td in sorted(GT):
    pf=os.path.join(D,f"picks_{td}.json")
    if not os.path.exists(pf): print(f"  {td:<12}   (agent not done yet)"); continue
    try: llm=json.load(open(pf))
    except:
        import re; llm=re.findall(r'\"([A-Z0-9&\-]+)\"',open(pf).read())
    rets=GT[td]["returns"]; ops=GT[td]["operator_picks"]
    inc=sum(1 for p in ops if p in llm); hits+=inc; tot+=len(ops)
    lr=[rets[s] for s in llm if s in rets]; orr=[rets[s] for s in ops if s in rets]; allr=list(rets.values())
    llm_ret=np.mean(lr) if lr else np.nan; op_ret=np.mean(orr) if orr else np.nan; base=np.mean(allr) if allr else np.nan
    llm_rets.append(llm_ret); op_rets.append(op_ret); base_rets.append(base)
    lw=(np.array(lr)>0).mean()*100 if lr else np.nan
    print(f"  {td:<12}{len(llm):>10}{inc}/{len(ops):<8}{llm_ret:>+10.2f}{op_ret:>+9.2f}{base:>+11.2f}{lw:>9.0f}%")
if tot:
    print("  "+"-"*72)
    print(f"  OVERALL recall@15 {hits/tot*100:.0f}%  (ML ceiling ~28%)  ·  LLM basket {np.nanmean(llm_rets):+.2f}%/day  ·  operator {np.nanmean(op_rets):+.2f}%/day  ·  baseline {np.nanmean(base_rets):+.2f}%/day")
    print(f"  LLM vs baseline edge: {np.nanmean(llm_rets)-np.nanmean(base_rets):+.2f}%/day  (positive = LLM reading picks better-than-random from the pool)")
