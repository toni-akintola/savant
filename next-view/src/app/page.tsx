import { DashboardShell } from "@/app/components/dashboard-shell";
import { UserProfiles } from "@/app/components/user-profiles";

export default function DashboardPage() {
  return (
    <DashboardShell>
      <UserProfiles />
    </DashboardShell>
  );
}
