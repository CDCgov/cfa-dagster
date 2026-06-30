# From Zensical to Azure Static Web App (SWA)

## Implementation Notes

* Make sure your website files (.html) are in one folder or else your app will not be deployed because the server will be trying to read ALL of your files.
* This implementation assumes you already have your website files and they are in a GitHub repository.

## Steps

### Creating the SWA

1. Go to portal.azure.com, log in with your ext.cdc.gov account, and click on "Create a resource"  
   <img width="168" height="161" alt="image" src="https://github.com/user-attachments/assets/92b503dd-674c-4aca-87c5-1169380efbdf" />  
2. Within the "Search services and marketplace" search bar, search for "static web app"  
   <img width="713" height="245" alt="image" src="https://github.com/user-attachments/assets/87da322a-672d-4e41-8fc5-661f132081be" />  
3. Click on the "Create" dropdown and select "Static Web App" from the "Static Web App" by Microsoft
   <img width="230" height="351" alt="image" src="https://github.com/user-attachments/assets/51aaee46-37eb-41e6-a3ad-5961131d747c" />  
4. If you are logged into your ext.cdc.gov account, you will see EXT-EDAV-CFA-PRD as an option for "Subscription". Select that, select "Static Web App" and click on the "Create" button.
   <img width="755" height="189" alt="image" src="https://github.com/user-attachments/assets/f04b8e94-9d89-4ebd-8a70-410040560927" />  
5. Under "Project Details", select EXT-EDAV-CFA-PRD as the Subscription and EXT-EDAV-CFA-DEV as the Resource Group.
   <img width="724" height="204" alt="image" src="https://github.com/user-attachments/assets/a1ec0c60-00a7-48b3-bb3b-25c4d8ee5c3c" />  
6. Enter the name of your website without using spaces, click on the Free plan, select your Deployment Source (for this demo I chose GitHub), and connect your GitHub account if using GitHub.  
   <img width="751" height="386" alt="image" src="https://github.com/user-attachments/assets/156a0551-88d7-4393-b37b-eacf914ee0f9" />  
7. Choose which GitHub organization, repository, and branch where your website files are.  
   <img width="736" height="123" alt="image" src="https://github.com/user-attachments/assets/284279f3-6142-4344-9696-a8d679822134" />  
8. Under "App location", write out the path to the website files. For me, this was within the `./docs/site` folder of my repository. Then, select the "Next: Deployment configuration" button.
   <img width="747" height="293" alt="image" src="https://github.com/user-attachments/assets/45195cff-3961-4d6b-8716-c3cc76b0249f" />    
9. On the Deployment configuration page, select GitHub as the Deployment authorization policy.
    <img width="709" height="185" alt="image" src="https://github.com/user-attachments/assets/30f3ec22-6b06-430c-b30d-15e30f8fec51" />  
10. Finally, go to the "Review + create" page to confirm your choices.
    <img width="570" height="489" alt="image" src="https://github.com/user-attachments/assets/f9641402-9c53-464e-a21e-b72d8d694b37" />

### GitHub Actions + Adding Microsoft Entra authentication

11. Inside your repository, you should see an automatically-generated `.yml` file within `./.github/workflows/` that starts with `azure-static-web-apps...`. This is your deployment file that tells GitHub Actions what to do.   
    <img width="918" height="327" alt="image" src="https://github.com/user-attachments/assets/89e6e5ca-be9e-4d8c-b0aa-2d00fe376fa1" />  
    Under the `Build and Deploy` job, make sure that your `app_location` is set to the exact folder with the `.html` files that you set in step 8.
    <img width="793" height="292" alt="image" src="https://github.com/user-attachments/assets/c0347b93-ac94-44de-8b39-c5291bb4f08f" />  
    Add `config_file_location: "<WHERE YOUR CONFIG FILE WILL LIVE>"`. For me, this was `./docs/site/`.  
12. Navigate to where you specified your `config_file_location` and create a config file called `staticwebapp.config.json` and add the following code:
    ```
    {
      "navigationFallback": {
        "rewrite": "/index.html"
      },
      "routes": [
        {
          "route": "/*",
          "allowedRoles": ["authenticated"]
        }
      ],
      "responseOverrides": {
        "401": {
          "statusCode": 302,
          "redirect": "/.auth/login/aad"
        }
      }
    }
    ```
   
### Verifying Successful Deployment

13. At the top of your GitHub repository, navigate to the "Actions" page.  
    <img width="738" height="109" alt="image" src="https://github.com/user-attachments/assets/3b35bdeb-a9c0-4e3a-9586-74032795af0c" />
    You should see a "Build and Deploy Job" that automatically runs any time you commit to the branch you specified in step 7.  
    <img width="917" height="419" alt="image" src="https://github.com/user-attachments/assets/f354c665-5226-4202-bc72-47de906083f6" />
14. Open up the "Build and Deploy" dropdown for more details on your website deployment. If the deployment is successful, you can find your website URL at the bottom of the "Build and Deploy" log.
    <img width="512" height="170" alt="image" src="https://github.com/user-attachments/assets/97564b95-1719-4772-81a8-26a5502d5e43" />
15. Your website's URL can also be found on the "Overview" page on Azure Portal for your SWA.
    <img width="935" height="534" alt="image" src="https://github.com/user-attachments/assets/d29bb85b-09e5-462e-85ca-22eddc59c3f8" />
 

    


 


