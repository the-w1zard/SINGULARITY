
import {  setSauron, } from '../the-cloud/the-cloud/hook-system'

// #region ── LIVE SELF BINDING ────────────────────────────────────────────────
export function createLiveSelf(instanceId: string) {
  const liveTarget: Record<string, any> = {};
  const selfProxy = new Proxy(liveTarget, {
    get(target, key: string) { return target[key]; },
    set(target, key: string, value: any) {// reuse the existing Sauron store — same primitive `live`/`useLive` already build on
      target[key] = value;
      setSauron(`${instanceId}:${key}`, value);
      return true;
    }
  });
  return { selfProxy, liveTarget };
}
// #endregion
