# logee-dwh

---

### Contents

1. How to create view for L1 or L2

---

## How to create view for L1 or L2

**First**, Switch to a new branch with the following command
```bash
git checkout -b feature/{l1 / l2}-{table-name}
```
for example: `feature/l1-lgd-product`

**Second**, open up `source/sql/dwh/bq_view` folder and verify whether the associated _L_ level and dataset name folder are available or not. Create the respective folder if it's not available. 

For example, if you are are about to create a new `L1` view for table `lgd_product` from `visibility` database. Then the script will be put inside `bq_view/L1/visibility/lgd_product.sql`

**Third**, put your `SELECT` query for the view under inside the directory like the example above.

**Fourth**, run `git add .` and `git commit -m "add some commit message here"`, and finally push it to GitHub with `git push`.

**Fifth**, go to [https://github.com/Logee-Data/logee-dwh/pulls](https://github.com/Logee-Data/logee-dwh/pulls) and open up a new Pull Request. 

Fill in the form as detail as possible, add the `Reviewers`, and select `Assignee` to yourself.

**Sixth**, copy the link of your Pull Request page and notify the respective `Reviewers` on Discord. Wait until it is accepted, and edit the resources as told by feedbacks from the Pull Request. Reviewer will merge it once all of them give the approval.

**Seventh**, once merged, check on BigQuery to verify whether your view is created or not. Contact your peers if you are not finding your view.

