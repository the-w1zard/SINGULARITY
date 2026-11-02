// neutrons/tags.ts
//                                    TAGS_NEUTRON
//  Completion note: the subscribe half existed (subscribeByTag / listTags / getTagsByChannel).
//  Missing was the write half: what tags a channel carries, and how a tagged publish reaches
//  the subscribers. tagChannel()/publishByTag() close that loop. No existing line was rewritten.
// #region ══════════════════════════════════════════════════════════════════════════════════════════════════════
export class TAGS_NEUTRON {
	public root_cell_key: ATOM_KEY;
	public tagSubs = new Map<string, Set<CSEHandler>>();
	public tagsByChannel = new Map<string, Set<string>>();

	constructor(root_cell_key: ATOM_KEY, _config: Record<S, unknown> = {}) {
		this.root_cell_key = root_cell_key;
	}

	// ── channel → tags ─────────────────────────────────────────────────────────────────────────
	/** Declare the tags a channel carries. Replaces any previous tag set for that channel. */
	public tagChannel(channel: S, tags: S[]): void {
		this.tagsByChannel.set(channel, new Set(tags));
	}
	public untagChannel(channel: S): B { return this.tagsByChannel.delete(channel); }
	public getTagsForChannel(channel: S): S[] { return [...(this.tagsByChannel.get(channel) ?? [])]; }

	/** All channels carrying at least one of the given tags. */
	public channelsByTags(tags: S[]): S[] {
		const want = new Set(tags);
		const out: S[] = [];
		for (const [ch, chTags] of this.tagsByChannel) for (const t of chTags) if (want.has(t)) { out.push(ch); break; }
		return out;
	}

	// ── subscriber side (existing) ──────────────────────────────────────────────────────────────
	public subscribeByTag(tag: string, handler: CSEHandler): () => void {
		const set = this.tagSubs.get(tag) ?? new Set<CSEHandler>(); set.add(handler); this.tagSubs.set(tag, set);
		return () => set.delete(handler);
	}
	public unsubscribeByTag(tag: string, handler: CSEHandler): B { return this.tagSubs.get(tag)?.delete(handler) ?? false; }
	public listTags(): string[] { return [...this.tagSubs.keys()] }

	// ── publish side (the missing half) ─────────────────────────────────────────────────────────
	/** Fan a tagged message out to every handler subscribed to a tag the channel carries. */
	public publishByTag(msg: CSEMessage): N {
		const tags = this.tagsByChannel.get(msg.rt);
		if (!tags || tags.size === 0) return 0;
		let delivered = 0;
		for (const tag of tags) {
			const subs = this.tagSubs.get(tag);
			if (!subs) continue;
			for (const handler of [...subs]) {
				try { handler(msg); delivered++; } catch (e) { console.error(`[tags-NEUTRON] tag handler failed for "${tag}":`, e); }
			}
		}
		return delivered;
	}

	public getTagsByChannel(): Record<string, string[]> {
		const out: Record<string, string[]> = {}
		for (const [ch, tags] of this.tagsByChannel) out[ch] = [...tags]
		return out
	}

	purge(): void { this.tagSubs.clear(); this.tagsByChannel.clear(); }
}
// #endregion
