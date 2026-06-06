import { z } from "zod";

export const healthStatusSchema = z.object({
  service: z.string().min(1),
  status: z.literal("milestone-0"),
  version: z.string().min(1),
});

export type HealthStatus = z.infer<typeof healthStatusSchema>;

export function parseHealthStatus(input: unknown): HealthStatus {
  return healthStatusSchema.parse(input);
}
