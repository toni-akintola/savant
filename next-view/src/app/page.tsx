import { DashboardShell } from "@/components/dashboard-shell";
import { UserProfiles } from "@/components/user-profiles";

export default function DashboardPage() {
  return (
    <DashboardShell>
      <div className="flex flex-col gap-4 w-full">
        <div className="space-y-2">
          <h1 className="text-3xl font-bold tracking-tight">Dashboard</h1>
          <p className="text-muted-foreground">
            Welcome to your user management dashboard. Browse and manage user
            profiles.
          </p>
        </div>
      </div>
      <UserProfiles />
    </DashboardShell>
  );
}
