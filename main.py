"""
taldbt - Talend to dbt Migration Tool

Usage:
    streamlit run taldbt/ui/app.py       # Launch the web UI
    python main.py discover <path>       # CLI discovery mode
    python main.py migrate <path> <out>  # CLI migration mode
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def main():
    args = sys.argv[1:]

    if not args or args[0] == "ui":
        os.system(f"streamlit run {os.path.join('taldbt', 'ui', 'app.py')}")
        return

    if args[0] == "discover" and len(args) >= 2:
        from taldbt.parsers.project_scanner import scan_project
        from taldbt.parsers.xml_parser import parse_job
        from taldbt.models.ast_models import ProjectAST, JobType
        from taldbt.graphing.dag_builder import apply_dag_to_project
        import json

        input_path = args[1]
        print(f"[taldbt] Scanning {input_path}...")

        scan = scan_project(input_path)
        project = ProjectAST(project_name=os.path.basename(input_path), input_path=input_path)

        for entry in scan["process_jobs"]:
            job = parse_job(entry["path"], entry["name"])
            project.jobs[entry["name"]] = job

        for entry in scan["joblets"]:
            job = parse_job(entry["path"], entry["name"], JobType.JOBLET)
            project.joblets[entry["name"]] = job

        apply_dag_to_project(project)

        # Output
        out_path = args[2] if len(args) > 2 else "migration_plan.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(project.model_dump(), f, indent=2, default=str)
        print(f"[taldbt] Discovery complete → {out_path}")

    elif args[0] == "migrate" and len(args) >= 3:
        from taldbt.parsers.project_scanner import scan_project
        from taldbt.parsers.xml_parser import parse_job
        from taldbt.models.ast_models import ProjectAST, JobType
        from taldbt.graphing.dag_builder import apply_dag_to_project
        from taldbt.codegen.model_assembler import assemble_model
        from taldbt.codegen.dbt_scaffolder import scaffold_dbt_project, write_model_file

        input_path = args[1]
        output_path = args[2]

        print(f"[taldbt] Migrating {input_path} → {output_path}")

        scan = scan_project(input_path)
        project = ProjectAST(project_name=os.path.basename(input_path), input_path=input_path)

        for entry in scan["process_jobs"]:
            job = parse_job(entry["path"], entry["name"])
            project.jobs[entry["name"]] = job

        apply_dag_to_project(project)
        scaffold_dbt_project(project, output_path)

        generated = 0
        for name, job in project.jobs.items():
            sql = assemble_model(job)
            if sql:
                write_model_file(sql, name, output_path)
                generated += 1
                print(f"  ✅ {name}")

        print(f"[taldbt] Done. {generated} models generated in {output_path}")

    else:
        print("Usage:")
        print("  python main.py ui                    # Launch web UI")
        print("  python main.py discover <talend_dir>  # Scan & analyze")
        print("  python main.py migrate <talend_dir> <output_dir>  # Full migration")


if __name__ == "__main__":
    main()
