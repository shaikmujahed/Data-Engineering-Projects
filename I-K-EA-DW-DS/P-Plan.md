# **Project Plan**
## **Information**
### **File Organization:**
  - All the files you need to work with are in the dlt/ folder:
    - Setup scripts: environment_setup.ipynb

# **Databricks Configuration Requirements**
To run this project end-to-end, complete the following setup steps in your Databricks workspace:

1. **Create a Databricks Account**
   - Sign up for a Databricks Free Edition account if you don’t already have one.
   - Familiarize yourself with the workspace, clusters, and notebook interface.

2. **Import this repository to Databricks**
   - In Databricks, go to the Workspace sidebar and click the "Repos" section, click "Add Repo".
      - Alternatively, go to your personal folder, click "create" and select "git folder".
   - Paste the GitHub URL for this repository.
   - Authenticate with GitHub if prompted, and select the main branch.
   - The repo will appear as a folder in your workspace, allowing you to edit, run notebooks, and manage files directly from Databricks.
   - For more details, see the official Databricks documentation: Repos in Databricks.
3. **Run the dlt/environment_setup.ipynb notebook to set up a catalog, schemas and volumes for the synthetic data generator.**
