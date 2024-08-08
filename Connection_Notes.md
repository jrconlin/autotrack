# Setting crap up

If you haven't already, download `gcloud`.

Create a local project profile. The $PROJECT name does not need to match the
GCP project, but it will probably help you keep things straight.
`gcloud config configurations create $PROJECT`

This creates a specific local project that will contain various settings for
gcloud. It's not strictly needed, but if you work on multiple projects like
I do, it's a god-send.

```bash
# Log into Google cloud (Make sure you're using the correct default credentials)
gcloud auth login
# Now log in using the application default (for when you run python stuff.)
gcloud auth application-default login
# set the default console project for this profile (This *is* the GCP project)
gcloud config set project $PROJECT
gcloud config set account $ACCOUNT_ID
```

verify that calling

```bash
cbt -instance $INSTANCE ls
```

returns the tables specified in the Google Developer Console.

## A few additional notes about default auth configurations.

Google permissions are fickle beasts. If done wrong, you can get strange errors like `cbt` seg-faulting.

Be sure to check that:

1. `GOOGLE_APPLICATION_CREDENTIALS` is either not set, or points to the
   correct JSON credential file for the project you're using.
2. Ensure that `google-cloud-sdk/bin` is in your PATH.
3. Ensure that you are not passing any additional `-creds` or
   `-auth-token` string to cbt.
4. If you have a `~/.cbtrc` file defined, ensure that it has the proper
   values for `project` and `instance` (alternately, you could create a
   `cbt.sh` script that includes these as options. e.g.

   ```bash
   #! /bin/bash
   $HOME/google-cloud-sdk/bin/cbt -project $PROJECT -instance $INSTANCE $*
   ```

   )

