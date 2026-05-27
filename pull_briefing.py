"""
pull_briefing.py — pull Philadelphia briefing from run faf88c1e

Run from /Users/ves/Documents/Claude/Projects/OSINT/project/:
    python3 pull_briefing.py

Outputs:
    philadelphia_briefing.json    — full 23-section briefing JSON
    philadelphia_briefing.md      — rendered markdown report
    philadelphia_run_status.json  — run metadata + entity/relationship counts
"""

import asyncio
import json
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

DATABASE_URL = os.environ["DATABASE_URL"]
RUN_PREFIX   = "faf88c1e"
OUT_DIR      = Path(__file__).parent

async def main():
    try:
        import asyncpg
    except ImportError:
        print("ERROR: asyncpg not installed. Run: pip install asyncpg")
        sys.exit(1)

    print(f"Connecting to Supabase...")
    conn = await asyncpg.connect(DATABASE_URL, ssl="require")

    # ── 1. Run record ─────────────────────────────────────────────────────────
    rows = await conn.fetch(
        """SELECT run_id::text, city_name, country_or_region, run_status,
                  started_at, completed_at, total_entities_found,
                  total_relationships_found, overall_confidence, failure_reason
             FROM agent_runs
            WHERE run_id::text LIKE $1
            ORDER BY started_at DESC LIMIT 5""",
        RUN_PREFIX + "%",
    )

    if not rows:
        print(f"ERROR: No run found with prefix {RUN_PREFIX}")
        await conn.close()
        sys.exit(1)

    run_record = dict(rows[0])
    run_id = run_record["run_id"]
    print(f"\n=== RUN STATUS ===")
    for k, v in run_record.items():
        print(f"  {k}: {v}")

    (OUT_DIR / "philadelphia_run_status.json").write_text(
        json.dumps({k: str(v) if v is not None else None for k, v in run_record.items()}, indent=2)
    )
    print(f"\n✓ Run status written to philadelphia_run_status.json")

    if run_record["run_status"] not in ("complete", "partial"):
        print(f"\nWARNING: Run status is '{run_record['run_status']}' — briefing may not exist yet.")

    # ── 2. Briefing from analytical_assessments ───────────────────────────────
    briefing_rows = await conn.fetch(
        """SELECT assessment_id::text, assessment_type, claim_text, claim_json,
                  created_at
             FROM analytical_assessments
            WHERE run_id = $1::uuid
              AND assessment_type IN ('final_briefing_full', 'final_briefing_summary')
            ORDER BY created_at DESC""",
        run_id,
    )

    print(f"\n=== BRIEFING RECORDS ===")
    print(f"Found {len(briefing_rows)} briefing assessment record(s)")

    if not briefing_rows:
        # Try broader search — maybe assessment_type differs
        all_types = await conn.fetch(
            """SELECT DISTINCT assessment_type, COUNT(*) as cnt
                 FROM analytical_assessments
                WHERE run_id = $1::uuid
                GROUP BY assessment_type
                ORDER BY cnt DESC""",
            run_id,
        )
        print("\nAll assessment_type values for this run:")
        for r in all_types:
            print(f"  {r['assessment_type']}: {r['cnt']} records")

        # Also check agent_outputs
        output_rows = await conn.fetch(
            """SELECT agent_name, output_type, output_key,
                      LEFT(output_value::text, 200) as preview,
                      created_at
                 FROM agent_outputs
                WHERE run_id = $1::uuid
                  AND agent_name = 'briefing_agent'
                ORDER BY created_at DESC LIMIT 5""",
            run_id,
        )
        print(f"\nagent_outputs rows for briefing_agent: {len(output_rows)}")
        for r in output_rows:
            print(f"  [{r['output_type']}] {r['output_key']}: {r['preview'][:100]}...")
    else:
        for row in briefing_rows:
            atype = row["assessment_type"]
            print(f"\n  Type: {atype}")
            print(f"  Created: {row['created_at']}")

            # claim_json holds the full briefing JSON
            if row["claim_json"]:
                cj = row["claim_json"]
                if isinstance(cj, str):
                    cj = json.loads(cj)
                if atype == "final_briefing_full":
                    out_path = OUT_DIR / "philadelphia_briefing.json"
                    out_path.write_text(json.dumps(cj, indent=2))
                    print(f"  ✓ Full briefing JSON written to philadelphia_briefing.json")

                    # Also render markdown if possible
                    try:
                        sys.path.insert(0, str(OUT_DIR))
                        from osint.agents.briefing import _render_markdown
                        md = _render_markdown(cj)
                        (OUT_DIR / "philadelphia_briefing.md").write_text(md)
                        print(f"  ✓ Markdown rendered to philadelphia_briefing.md")
                    except Exception as e:
                        print(f"  ! Markdown render failed: {e}")
                        # Write claim_text as fallback
                        if row["claim_text"]:
                            (OUT_DIR / "philadelphia_briefing.md").write_text(row["claim_text"])
                            print(f"  ✓ claim_text written to philadelphia_briefing.md (truncated)")

            elif row["claim_text"]:
                # Fallback: claim_text may hold markdown
                print(f"  claim_text length: {len(row['claim_text'])} chars")
                (OUT_DIR / "philadelphia_briefing.md").write_text(row["claim_text"])
                print(f"  ✓ claim_text written to philadelphia_briefing.md")

    # ── 3. Quick stats ────────────────────────────────────────────────────────
    entity_count = await conn.fetchval(
        "SELECT COUNT(*) FROM entities WHERE $1::uuid = ANY(source_run_ids)",
        run_id,
    )
    rel_count = await conn.fetchval(
        "SELECT COUNT(*) FROM relationships WHERE run_id = $1::uuid",
        run_id,
    )
    print(f"\n=== QUICK STATS ===")
    print(f"  Entities: {entity_count}")
    print(f"  Relationships: {rel_count}")

    # Entity type breakdown
    type_rows = await conn.fetch(
        """SELECT entity_type, COUNT(*) as cnt
             FROM entities
            WHERE $1::uuid = ANY(source_run_ids)
            GROUP BY entity_type
            ORDER BY cnt DESC""",
        run_id,
    )
    print("\n  Entity type breakdown:")
    for r in type_rows:
        print(f"    {r['entity_type']}: {r['cnt']}")

    await conn.close()
    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
