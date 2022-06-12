# logee-dwh

### Contents

1. [How to create view for L1 or L2](#how-to-create-views-for-l1-or-l2)

---


## How to create Views for L1 or L2

1. Make sure that you are on the `main` branch by running the `git branch` command.
2. Switch to a new branch with the following command: `bash git checkout -b feature/{l1 / l2}-{table-name}`. For example: `feature/l1-lgd-product`
3. Open up `source/sql/dwh/bq_view` folder and verify whether the associated _L_ level and dataset name folder are available or not. Create the respective folder if it's not available. 
   1. For example, if you are are about to create a new `L1` View for table `lgd_product` from `visibility` database. Then the script will be put inside `bq_view/L1/visibility/lgd_product.sql`
4. Put your query for the View under inside the directory like the example above.
5. Run `git add .` and `git commit -m "add some commit message here"`, and finally push it to GitHub with `git push`.
6. Go to [https://github.com/Logee-Data/logee-dwh/pulls](https://github.com/Logee-Data/logee-dwh/pulls) and open up a new Pull Request. 
   1. Fill in the form as detail as possible, add the `Reviewers`, and select `Assignee` to yourself.
7. Copy the link of your Pull Request page and notify the respective `Reviewers` on Discord. Wait until it is accepted, and edit the resources as told by feedbacks from the Pull Request. Reviewer will merge it once all of them give the approval.
8. Once merged, check on BigQuery to verify whether your View is created or not. Contact your peers if you are not finding your view.

