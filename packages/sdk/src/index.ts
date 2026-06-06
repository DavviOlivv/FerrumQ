export interface SdkStatus {
  packageName: "@ferrumq/sdk";
  status: "milestone-0";
}

export function sdkStatus(): SdkStatus {
  return {
    packageName: "@ferrumq/sdk",
    status: "milestone-0",
  };
}
